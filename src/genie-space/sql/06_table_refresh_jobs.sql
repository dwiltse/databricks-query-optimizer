-- Table Refresh Jobs - Replaces Materialized View Auto-Refresh
-- This script contains the refresh logic for performance tables
-- Run these as scheduled jobs (hourly/daily) instead of DLT pipeline

USE mcp.query_optimization;

-- =============================================================================
-- REFRESH query_performance_categorized (Every 15 minutes)
-- =============================================================================
CREATE OR REPLACE PROCEDURE refresh_query_performance_categorized()
LANGUAGE SQL
AS
$$
BEGIN
  -- Get latest timestamp to do incremental refresh
  DECLARE latest_timestamp TIMESTAMP DEFAULT (
    SELECT COALESCE(MAX(end_time), '1900-01-01 00:00:00') 
    FROM query_performance_categorized
  );
  
  -- Insert new categorized query data
  INSERT INTO query_performance_categorized
  SELECT
    statement_id,
    statement_text,
    executed_by,
    executed_as,
    total_duration_ms,
    execution_duration_ms,
    result_fetch_duration_ms,
    read_bytes,
    read_rows,
    read_partitions,
    error_message,
    compute.warehouse_id as warehouse_id,
    compute.type as compute_type,
    end_time,
    -- Performance categorization using your business logic
    CASE
      WHEN execution_duration_ms > 300000 THEN 'SLOW'
      WHEN execution_duration_ms > 60000 THEN 'MODERATE'
      ELSE 'FAST'
    END AS performance_category,
    -- Efficiency metric
    CASE
      WHEN read_bytes > 0 AND read_rows > 0 THEN read_bytes / read_rows
      ELSE NULL
    END AS bytes_per_row_efficiency,
    -- Optimization flags  
    CASE
      WHEN error_message IS NOT NULL THEN 'ERROR'
      WHEN result_fetch_duration_ms > 30000 THEN 'SLOW_FETCH'
      ELSE 'HEALTHY'
    END AS optimization_flag,
    CURRENT_TIMESTAMP() as created_at
  FROM system.query.history
  WHERE end_time > latest_timestamp
    AND end_time >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
    AND compute.warehouse_id IS NOT NULL;
    
  SELECT 'query_performance_categorized refreshed successfully' as status;
END
$$;

-- =============================================================================
-- REFRESH current_slow_queries (Every 15 minutes)  
-- =============================================================================
CREATE OR REPLACE PROCEDURE refresh_current_slow_queries()
LANGUAGE SQL
AS
$$
BEGIN
  -- Clear old data (keep last 24 hours)
  DELETE FROM current_slow_queries 
  WHERE first_seen < CURRENT_TIMESTAMP() - INTERVAL 24 HOURS;
  
  -- Insert/Update current slow queries
  MERGE INTO current_slow_queries AS target
  USING (
    SELECT 
      statement_id,
      executed_by,
      warehouse_id,
      execution_duration_ms,
      read_bytes,
      -- Performance impact score (1-10)
      CASE 
        WHEN execution_duration_ms > 1800000 THEN 10  -- 30+ minutes
        WHEN execution_duration_ms > 900000 THEN 8    -- 15+ minutes  
        WHEN execution_duration_ms > 300000 THEN 6    -- 5+ minutes
        ELSE 3
      END as performance_impact_score,
      -- Suggested optimization based on patterns
      CASE
        WHEN statement_text LIKE '%SELECT *%' THEN 'Replace SELECT * with specific columns'
        WHEN statement_text LIKE '%ORDER BY%' AND statement_text NOT LIKE '%LIMIT%' THEN 'Add LIMIT clause to ORDER BY'
        WHEN read_bytes / NULLIF(read_rows, 0) > 100000 THEN 'Optimize data access - high bytes per row'
        ELSE 'Review query execution plan'
      END as suggested_optimization,
      end_time as first_seen
    FROM system.query.history
    WHERE execution_duration_ms > 300000  -- SLOW queries only
      AND end_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
      AND compute.warehouse_id IS NOT NULL
  ) AS source
  ON target.statement_id = source.statement_id
  WHEN MATCHED THEN UPDATE SET
    occurrence_count = target.occurrence_count + 1,
    created_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    statement_id, executed_by, warehouse_id, execution_duration_ms, read_bytes,
    performance_impact_score, suggested_optimization, first_seen, occurrence_count, created_at
  ) VALUES (
    source.statement_id, source.executed_by, source.warehouse_id, source.execution_duration_ms, 
    source.read_bytes, source.performance_impact_score, source.suggested_optimization, 
    source.first_seen, 1, CURRENT_TIMESTAMP()
  );
  
  SELECT 'current_slow_queries refreshed successfully' as status;
END
$$;

-- =============================================================================
-- REFRESH hourly_performance (Every hour)
-- =============================================================================
CREATE OR REPLACE PROCEDURE refresh_hourly_performance()
LANGUAGE SQL  
AS
$$
BEGIN
  -- Get the latest hour already processed
  DECLARE latest_hour TIMESTAMP DEFAULT (
    SELECT COALESCE(
      TIMESTAMP(MAX(query_date) + INTERVAL 1 DAY, MAX(query_hour)) - INTERVAL 1 HOUR,
      CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
    )
    FROM hourly_performance
  );
  
  -- Insert hourly aggregations for new data
  INSERT INTO hourly_performance
  SELECT 
    DATE(start_time) as query_date,
    HOUR(start_time) as query_hour,
    workspace_id,
    user_id,
    COUNT(*) as query_count,
    COUNT(DISTINCT query_hash) as unique_query_patterns,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as median_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as p99_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    SUM(bytes_read) as total_bytes_read,
    AVG(bytes_read) as avg_bytes_read,
    SUM(rows_read) as total_rows_read,
    AVG(rows_read) as avg_rows_read,
    COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) as successful_queries,
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries,
    CAST(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) AS DECIMAL(10,4)) / COUNT(*) as success_rate,
    AVG(complexity_score) as avg_complexity_score,
    AVG(optimization_score) as avg_optimization_score,
    CURRENT_TIMESTAMP() as created_at
  FROM query_performance_raw
  WHERE start_time > latest_hour
    AND start_time >= CURRENT_DATE() - INTERVAL 90 DAYS
    AND DATE_TRUNC('HOUR', start_time) < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())
  GROUP BY DATE(start_time), HOUR(start_time), workspace_id, user_id;
  
  SELECT 'hourly_performance refreshed successfully' as status;
END
$$;

-- =============================================================================
-- REFRESH daily_performance (Once daily)
-- =============================================================================
CREATE OR REPLACE PROCEDURE refresh_daily_performance()
LANGUAGE SQL
AS
$$
BEGIN
  -- Get the latest date already processed
  DECLARE latest_date DATE DEFAULT (
    SELECT COALESCE(MAX(query_date), CURRENT_DATE() - INTERVAL 90 DAYS)
    FROM daily_performance
  );
  
  -- Insert daily aggregations for new dates
  INSERT INTO daily_performance
  SELECT 
    DATE(start_time) as query_date,
    workspace_id,
    user_id,
    COUNT(*) as total_queries,
    COUNT(DISTINCT query_hash) as unique_patterns,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    SUM(bytes_read) as total_bytes_read,
    COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) as successful_queries,
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries,
    CAST(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) AS DECIMAL(10,4)) / COUNT(*) as success_rate,
    -- Performance categories using your business logic
    COUNT(CASE WHEN duration_ms < 60000 THEN 1 END) as fast_queries,
    COUNT(CASE WHEN duration_ms BETWEEN 60000 AND 300000 THEN 1 END) as medium_queries,
    COUNT(CASE WHEN duration_ms > 300000 THEN 1 END) as slow_queries,
    -- Cost categories
    COUNT(CASE WHEN compute_cost_dbu < 1 THEN 1 END) as low_cost_queries,
    COUNT(CASE WHEN compute_cost_dbu BETWEEN 1 AND 10 THEN 1 END) as medium_cost_queries,
    COUNT(CASE WHEN compute_cost_dbu > 10 THEN 1 END) as high_cost_queries,
    -- Efficiency metrics
    SUM(compute_cost_dbu) / NULLIF(SUM(duration_ms), 0) * 1000 as cost_per_second,
    SUM(bytes_read) / NULLIF(SUM(duration_ms), 0) * 1000 as bytes_per_second,
    AVG(optimization_score) as avg_optimization_score,
    CURRENT_TIMESTAMP() as created_at
  FROM query_performance_raw
  WHERE DATE(start_time) > latest_date
    AND start_time >= CURRENT_DATE() - INTERVAL 90 DAYS
    AND DATE(start_time) < CURRENT_DATE()  -- Don't process today until it's complete
  GROUP BY DATE(start_time), workspace_id, user_id;
  
  SELECT 'daily_performance refreshed successfully' as status;
END
$$;

-- =============================================================================
-- Master refresh procedure (call this from scheduled job)
-- =============================================================================
CREATE OR REPLACE PROCEDURE refresh_all_performance_tables()
LANGUAGE SQL
AS
$$
BEGIN
  CALL refresh_query_performance_categorized();
  CALL refresh_current_slow_queries();
  CALL refresh_hourly_performance();
  CALL refresh_daily_performance();
  
  SELECT 'All performance tables refreshed successfully' as status;
END
$$;

-- =============================================================================
-- Scheduling Instructions
-- =============================================================================

-- Create Databricks Job with these schedules:
-- 1. Every 15 minutes: CALL refresh_query_performance_categorized(); CALL refresh_current_slow_queries();
-- 2. Every hour: CALL refresh_hourly_performance();  
-- 3. Daily at 1 AM: CALL refresh_daily_performance();
-- 
-- OR run master procedure every 15 minutes:
-- CALL mcp.query_optimization.refresh_all_performance_tables();

-- Grant permissions
GRANT EXECUTE ON PROCEDURE refresh_query_performance_categorized TO `query-optimization-users`;
GRANT EXECUTE ON PROCEDURE refresh_current_slow_queries TO `query-optimization-users`;  
GRANT EXECUTE ON PROCEDURE refresh_hourly_performance TO `query-optimization-users`;
GRANT EXECUTE ON PROCEDURE refresh_daily_performance TO `query-optimization-users`;
GRANT EXECUTE ON PROCEDURE refresh_all_performance_tables TO `query-optimization-users`;