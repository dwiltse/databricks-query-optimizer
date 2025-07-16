-- ETL Pipeline for Query Performance Data Processing
-- This script handles incremental data processing from system tables

USE mcp.query_optimization;

-- Helper function to calculate query hash for pattern matching
CREATE OR REPLACE FUNCTION calculate_query_hash(query_text STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Calculate hash for query pattern matching'
AS
$$
  -- Normalize query text for pattern matching
  SELECT SHA2(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              UPPER(TRIM(query_text)),
              '[0-9]+', 'N'  -- Replace numbers with N
            ),
            '\'[^\']*\'', 'S'  -- Replace string literals with S
          ),
          '\\s+', ' '  -- Normalize whitespace
        ),
        '--[^\n]*\n', ' '  -- Remove single-line comments
      ),
      '/\\*.*?\\*/', ' '  -- Remove multi-line comments
    ),
    256
  )
$$;

-- Helper function to calculate query complexity score
CREATE OR REPLACE FUNCTION calculate_complexity_score(query_text STRING)
RETURNS DECIMAL(5,2)
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Calculate query complexity score (1-10)'
AS
$$
  SELECT LEAST(10, GREATEST(1, 
    1 + 
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'SELECT', ''))) / 6) * 0.5 +
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'JOIN', ''))) / 4) * 1.0 +
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WHERE', ''))) / 5) * 0.3 +
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'GROUP BY', ''))) / 8) * 0.8 +
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'ORDER BY', ''))) / 8) * 0.6 +
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'UNION', ''))) / 5) * 0.7 +
    ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WINDOW', ''))) / 6) * 0.9 +
    (LENGTH(query_text) / 1000) * 0.1
  ))
$$;

-- Helper function to calculate optimization score
CREATE OR REPLACE FUNCTION calculate_optimization_score(query_text STRING, duration_ms BIGINT, bytes_read BIGINT, compute_cost_dbu DECIMAL(10,4))
RETURNS DECIMAL(5,2)
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Calculate query optimization score (1-10, higher is better)'
AS
$$
  SELECT 
    LEAST(10, GREATEST(1,
      10 -
      -- Penalty for inefficient patterns
      (CASE WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 2 ELSE 0 END) -
      (CASE WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 3 ELSE 0 END) -
      (CASE WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 4 ELSE 0 END) -
      (CASE WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 1 ELSE 0 END) -
      (CASE WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 1 ELSE 0 END) -
      (CASE WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 1 ELSE 0 END) -
      -- Penalty for performance issues
      (CASE WHEN duration_ms > 300000 THEN 2 ELSE 0 END) -
      (CASE WHEN compute_cost_dbu > 20 THEN 2 ELSE 0 END) -
      (CASE WHEN bytes_read > 5368709120 THEN 1 ELSE 0 END) -
      -- Penalty for very long queries (complexity)
      (CASE WHEN LENGTH(query_text) > 10000 THEN 1 ELSE 0 END)
    ))
$$;

-- ETL Procedure: Incremental data processing
CREATE OR REPLACE PROCEDURE process_query_performance_incremental(
  start_timestamp TIMESTAMP,
  end_timestamp TIMESTAMP
)
LANGUAGE SQL
COMMENT 'Process query performance data incrementally from system tables'
AS
$$
BEGIN
  -- Log start of processing
  INSERT INTO mcp.query_optimization.etl_log (
    process_name,
    start_time,
    status,
    message
  ) VALUES (
    'process_query_performance_incremental',
    CURRENT_TIMESTAMP(),
    'STARTED',
    CONCAT('Processing data from ', start_timestamp, ' to ', end_timestamp)
  );

  -- Process raw query performance data
  INSERT INTO query_performance_raw (
    query_id,
    workspace_id,
    user_id,
    user_email,
    query_text,
    query_hash,
    start_time,
    end_time,
    duration_ms,
    rows_read,
    bytes_read,
    rows_produced,
    compute_cost_dbu,
    execution_status,
    error_message,
    cluster_id,
    warehouse_id,
    query_type,
    complexity_score,
    optimization_score,
    created_at,
    updated_at
  )
  SELECT 
    qh.query_id,
    qh.workspace_id,
    qh.user_id,
    qh.user_email,
    qh.query_text,
    calculate_query_hash(qh.query_text) as query_hash,
    qh.start_time,
    qh.end_time,
    qh.duration_ms,
    qh.rows_read,
    qh.bytes_read,
    qh.rows_produced,
    qh.compute_cost_dbu,
    qh.execution_status,
    qh.error_message,
    qh.cluster_id,
    qh.warehouse_id,
    -- Extract query type from query text
    CASE 
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'SELECT%' THEN 'SELECT'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'INSERT%' THEN 'INSERT'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'UPDATE%' THEN 'UPDATE'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'DELETE%' THEN 'DELETE'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'CREATE%' THEN 'CREATE'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'ALTER%' THEN 'ALTER'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'DROP%' THEN 'DROP'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'MERGE%' THEN 'MERGE'
      WHEN UPPER(TRIM(qh.query_text)) LIKE 'COPY%' THEN 'COPY'
      ELSE 'OTHER'
    END as query_type,
    calculate_complexity_score(qh.query_text) as complexity_score,
    calculate_optimization_score(qh.query_text, qh.duration_ms, qh.bytes_read, qh.compute_cost_dbu) as optimization_score,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
  FROM system.query.history qh
  WHERE qh.start_time >= start_timestamp
    AND qh.start_time < end_timestamp
    AND qh.query_id NOT IN (
      SELECT query_id FROM query_performance_raw 
      WHERE start_time >= start_timestamp AND start_time < end_timestamp
    );

  -- Update query patterns
  MERGE INTO query_patterns qp
  USING (
    SELECT 
      query_hash,
      -- Determine pattern type
      CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'SELECT_ALL'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'CARTESIAN_JOIN'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'UNPARTITIONED_FILTER'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'REDUNDANT_DISTINCT'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'UNION_OPTIMIZATION'
        WHEN complexity_score > 7 THEN 'HIGH_COMPLEXITY'
        WHEN avg_duration_ms > 300000 THEN 'LONG_RUNNING'
        WHEN avg_cost_dbu > 20 THEN 'HIGH_COST'
        ELSE 'STANDARD'
      END as pattern_type,
      -- Pattern description
      CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'Query uses SELECT * which may retrieve unnecessary columns'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'Query uses ORDER BY without LIMIT, sorting entire dataset'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'Query may have Cartesian JOIN without proper conditions'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'Query filters may not utilize partitioning'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'Query has redundant DISTINCT with GROUP BY'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'Query uses UNION instead of UNION ALL'
        WHEN complexity_score > 7 THEN 'High complexity query that may benefit from simplification'
        WHEN avg_duration_ms > 300000 THEN 'Long running query that needs performance optimization'
        WHEN avg_cost_dbu > 20 THEN 'High cost query that needs cost optimization'
        ELSE 'Standard query pattern'
      END as pattern_description,
      -- Create query template by replacing literals
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(query_text, '[0-9]+', '?'),
          '\'[^\']*\'', '?'
        ),
        '"[^"]*"', '?'
      ) as query_template,
      MIN(start_time) as first_seen,
      MAX(start_time) as last_seen,
      COUNT(*) as occurrence_count,
      AVG(duration_ms) as avg_duration_ms,
      AVG(compute_cost_dbu) as avg_cost_dbu,
      -- Optimization priority
      CASE 
        WHEN AVG(compute_cost_dbu) > 50 OR AVG(duration_ms) > 600000 THEN 'HIGH'
        WHEN AVG(compute_cost_dbu) > 20 OR AVG(duration_ms) > 300000 THEN 'MEDIUM'
        ELSE 'LOW'
      END as optimization_priority,
      -- Optimization recommendations
      CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'Replace SELECT * with specific column names'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'Add LIMIT clause to ORDER BY queries'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'Add proper JOIN conditions'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'Add partition filters to WHERE clause'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'Remove redundant DISTINCT'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'Use UNION ALL when appropriate'
        WHEN complexity_score > 7 THEN 'Consider breaking down complex query into simpler parts'
        WHEN avg_duration_ms > 300000 THEN 'Review query execution plan and consider indexing'
        WHEN avg_cost_dbu > 20 THEN 'Optimize data access patterns and consider caching'
        ELSE 'Review for general optimization opportunities'
      END as optimization_recommendations
    FROM query_performance_raw
    WHERE start_time >= start_timestamp
      AND start_time < end_timestamp
      AND execution_status = 'FINISHED'
    GROUP BY query_hash, query_text, complexity_score
  ) src
  ON qp.query_hash = src.query_hash
  WHEN MATCHED THEN
    UPDATE SET 
      last_seen = src.last_seen,
      occurrence_count = qp.occurrence_count + src.occurrence_count,
      avg_duration_ms = (qp.avg_duration_ms * qp.occurrence_count + src.avg_duration_ms * src.occurrence_count) / (qp.occurrence_count + src.occurrence_count),
      avg_cost_dbu = (qp.avg_cost_dbu * qp.occurrence_count + src.avg_cost_dbu * src.occurrence_count) / (qp.occurrence_count + src.occurrence_count),
      optimization_priority = src.optimization_priority,
      optimization_recommendations = src.optimization_recommendations,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      pattern_id,
      query_hash,
      pattern_type,
      pattern_description,
      query_template,
      first_seen,
      last_seen,
      occurrence_count,
      avg_duration_ms,
      avg_cost_dbu,
      optimization_priority,
      optimization_recommendations,
      created_at,
      updated_at
    ) VALUES (
      UUID(),
      src.query_hash,
      src.pattern_type,
      src.pattern_description,
      src.query_template,
      src.first_seen,
      src.last_seen,
      src.occurrence_count,
      src.avg_duration_ms,
      src.avg_cost_dbu,
      src.optimization_priority,
      src.optimization_recommendations,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  -- Refresh materialized views
  REFRESH MATERIALIZED VIEW mv_hourly_performance;
  REFRESH MATERIALIZED VIEW mv_daily_performance;
  REFRESH MATERIALIZED VIEW mv_pattern_performance;
  REFRESH MATERIALIZED VIEW mv_user_performance;
  REFRESH MATERIALIZED VIEW mv_performance_alerts;

  -- Log completion
  INSERT INTO mcp.query_optimization.etl_log (
    process_name,
    start_time,
    end_time,
    status,
    message,
    records_processed
  ) VALUES (
    'process_query_performance_incremental',
    start_timestamp,
    CURRENT_TIMESTAMP(),
    'COMPLETED',
    CONCAT('Successfully processed data from ', start_timestamp, ' to ', end_timestamp),
    (SELECT COUNT(*) FROM query_performance_raw WHERE start_time >= start_timestamp AND start_time < end_timestamp)
  );

EXCEPTION
  WHEN OTHER THEN
    -- Log error
    INSERT INTO mcp.query_optimization.etl_log (
      process_name,
      start_time,
      end_time,
      status,
      message,
      error_message
    ) VALUES (
      'process_query_performance_incremental',
      start_timestamp,
      CURRENT_TIMESTAMP(),
      'FAILED',
      CONCAT('Failed to process data from ', start_timestamp, ' to ', end_timestamp),
      SQLERRM
    );
    
    -- Re-raise the exception
    RAISE;
END;
$$;

-- ETL Procedure: Update performance baselines
CREATE OR REPLACE PROCEDURE update_performance_baselines()
LANGUAGE SQL
COMMENT 'Update performance baselines for anomaly detection'
AS
$$
BEGIN
  -- Log start of processing
  INSERT INTO mcp.query_optimization.etl_log (
    process_name,
    start_time,
    status,
    message
  ) VALUES (
    'update_performance_baselines',
    CURRENT_TIMESTAMP(),
    'STARTED',
    'Updating performance baselines for anomaly detection'
  );

  -- Update or insert performance baselines
  MERGE INTO performance_baselines pb
  USING (
    SELECT 
      query_hash,
      workspace_id,
      user_id,
      current_date() - INTERVAL 30 DAYS as baseline_period_start,
      current_date() - INTERVAL 1 DAY as baseline_period_end,
      AVG(duration_ms) as baseline_avg_duration_ms,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as baseline_p95_duration_ms,
      AVG(compute_cost_dbu) as baseline_avg_cost_dbu,
      CAST(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) AS DECIMAL(10,4)) / COUNT(*) as baseline_success_rate,
      COUNT(*) as baseline_execution_count,
      -- Set thresholds at 2x the 95th percentile
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) * 2 as threshold_duration_ms,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY compute_cost_dbu) * 2 as threshold_cost_dbu
    FROM query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 30 DAYS
      AND start_time < current_date()
      AND execution_status IN ('FINISHED', 'FAILED')
    GROUP BY query_hash, workspace_id, user_id
    HAVING COUNT(*) >= 5  -- Minimum executions for baseline
  ) src
  ON pb.query_hash = src.query_hash 
     AND pb.workspace_id = src.workspace_id 
     AND pb.user_id = src.user_id
     AND pb.baseline_period_end = src.baseline_period_end
  WHEN MATCHED THEN
    UPDATE SET 
      baseline_avg_duration_ms = src.baseline_avg_duration_ms,
      baseline_p95_duration_ms = src.baseline_p95_duration_ms,
      baseline_avg_cost_dbu = src.baseline_avg_cost_dbu,
      baseline_success_rate = src.baseline_success_rate,
      baseline_execution_count = src.baseline_execution_count,
      threshold_duration_ms = src.threshold_duration_ms,
      threshold_cost_dbu = src.threshold_cost_dbu,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      baseline_id,
      query_hash,
      workspace_id,
      user_id,
      baseline_period_start,
      baseline_period_end,
      baseline_avg_duration_ms,
      baseline_p95_duration_ms,
      baseline_avg_cost_dbu,
      baseline_success_rate,
      baseline_execution_count,
      threshold_duration_ms,
      threshold_cost_dbu,
      created_at,
      updated_at
    ) VALUES (
      UUID(),
      src.query_hash,
      src.workspace_id,
      src.user_id,
      src.baseline_period_start,
      src.baseline_period_end,
      src.baseline_avg_duration_ms,
      src.baseline_p95_duration_ms,
      src.baseline_avg_cost_dbu,
      src.baseline_success_rate,
      src.baseline_execution_count,
      src.threshold_duration_ms,
      src.threshold_cost_dbu,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  -- Log completion
  INSERT INTO mcp.query_optimization.etl_log (
    process_name,
    start_time,
    end_time,
    status,
    message,
    records_processed
  ) VALUES (
    'update_performance_baselines',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'COMPLETED',
    'Successfully updated performance baselines',
    (SELECT COUNT(*) FROM performance_baselines WHERE updated_at >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR)
  );

EXCEPTION
  WHEN OTHER THEN
    -- Log error
    INSERT INTO mcp.query_optimization.etl_log (
      process_name,
      start_time,
      end_time,
      status,
      message,
      error_message
    ) VALUES (
      'update_performance_baselines',
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP(),
      'FAILED',
      'Failed to update performance baselines',
      SQLERRM
    );
    
    -- Re-raise the exception
    RAISE;
END;
$$;

-- ETL logging table
CREATE TABLE IF NOT EXISTS etl_log (
  log_id STRING DEFAULT UUID(),
  process_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  message STRING,
  records_processed BIGINT,
  error_message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (DATE(start_time))
COMMENT 'ETL process logging for monitoring and debugging';

-- Example usage and scheduling
-- Run this daily to process the previous day's data
-- CALL process_query_performance_incremental(
--   CURRENT_DATE() - INTERVAL 1 DAY,
--   CURRENT_DATE()
-- );

-- Run this weekly to update performance baselines
-- CALL update_performance_baselines();

-- Data retention procedure
CREATE OR REPLACE PROCEDURE cleanup_old_data()
LANGUAGE SQL
COMMENT 'Clean up old data based on retention policies'
AS
$$
BEGIN
  -- Delete query performance data older than 90 days
  DELETE FROM query_performance_raw
  WHERE start_time < current_date() - INTERVAL 90 DAYS;
  
  -- Delete query patterns not seen in last 90 days
  DELETE FROM query_patterns
  WHERE last_seen < current_date() - INTERVAL 90 DAYS;
  
  -- Delete old performance baselines
  DELETE FROM performance_baselines
  WHERE baseline_period_end < current_date() - INTERVAL 90 DAYS;
  
  -- Delete old ETL logs
  DELETE FROM etl_log
  WHERE start_time < current_date() - INTERVAL 30 DAYS;
  
  -- Optimize tables
  OPTIMIZE query_performance_raw;
  OPTIMIZE query_patterns;
  OPTIMIZE performance_baselines;
  OPTIMIZE etl_log;
END;
$$;