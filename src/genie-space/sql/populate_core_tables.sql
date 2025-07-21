-- Populate Core Tables - Simple ETL for POC Testing
-- This script populates the core tables (Step 2) from system tables
-- Run this BEFORE running the refresh_performance_tables.py notebook

USE mcp.query_optimization;

-- =============================================================================
-- Helper Functions (reusable logic)
-- =============================================================================

-- Calculate query hash for pattern matching
CREATE OR REPLACE FUNCTION calculate_query_hash(query_text STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Calculate hash for query pattern matching'
AS
$$
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

-- Calculate query complexity score (1-10)
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

-- Calculate optimization score (1-10, higher is better)
CREATE OR REPLACE FUNCTION calculate_optimization_score(query_text STRING, duration_ms BIGINT, bytes_read BIGINT)
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
      (CASE WHEN bytes_read > 5368709120 THEN 1 ELSE 0 END) -
      -- Penalty for very long queries (complexity)
      (CASE WHEN LENGTH(query_text) > 10000 THEN 1 ELSE 0 END)
    ))
$$;

-- =============================================================================
-- Populate query_performance_raw (from system.query.history)
-- =============================================================================

INSERT OVERWRITE query_performance_raw
SELECT 
    statement_id as query_id,
    workspace_id,
    user_id,
    executed_by as user_email,
    statement_text as query_text,
    calculate_query_hash(statement_text) as query_hash,
    start_time,
    end_time,
    execution_duration_ms as duration_ms,
    read_rows as rows_read,
    read_bytes as bytes_read,
    produced_rows as rows_produced,
    -- Approximate compute cost (you may need to adjust this calculation)
    CAST(execution_duration_ms AS DOUBLE) / 3600000 * 2.5 as compute_cost_dbu,
    CASE 
        WHEN error_message IS NULL THEN 'FINISHED'
        ELSE 'FAILED'
    END as execution_status,
    error_message,
    compute.cluster_id as cluster_id,
    compute.warehouse_id as warehouse_id,
    -- Extract query type from query text
    CASE 
        WHEN UPPER(TRIM(statement_text)) LIKE 'SELECT%' THEN 'SELECT'
        WHEN UPPER(TRIM(statement_text)) LIKE 'INSERT%' THEN 'INSERT'
        WHEN UPPER(TRIM(statement_text)) LIKE 'UPDATE%' THEN 'UPDATE'
        WHEN UPPER(TRIM(statement_text)) LIKE 'DELETE%' THEN 'DELETE'
        WHEN UPPER(TRIM(statement_text)) LIKE 'CREATE%' THEN 'CREATE'
        WHEN UPPER(TRIM(statement_text)) LIKE 'ALTER%' THEN 'ALTER'
        WHEN UPPER(TRIM(statement_text)) LIKE 'DROP%' THEN 'DROP'
        WHEN UPPER(TRIM(statement_text)) LIKE 'MERGE%' THEN 'MERGE'
        WHEN UPPER(TRIM(statement_text)) LIKE 'COPY%' THEN 'COPY'
        ELSE 'OTHER'
    END as query_type,
    calculate_complexity_score(statement_text) as complexity_score,
    calculate_optimization_score(statement_text, execution_duration_ms, read_bytes) as optimization_score,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
    AND statement_text IS NOT NULL
    AND execution_duration_ms IS NOT NULL;

-- =============================================================================
-- Populate query_patterns (from processed raw data)
-- =============================================================================

INSERT OVERWRITE query_patterns
SELECT 
    UUID() as pattern_id,
    query_hash,
    -- Determine pattern type based on query characteristics
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'SELECT_ALL'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'CARTESIAN_JOIN'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'UNPARTITIONED_FILTER'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'REDUNDANT_DISTINCT'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'UNION_OPTIMIZATION'
        WHEN AVG(complexity_score) > 7 THEN 'HIGH_COMPLEXITY'
        WHEN AVG(duration_ms) > 300000 THEN 'LONG_RUNNING'
        WHEN AVG(compute_cost_dbu) > 20 THEN 'HIGH_COST'
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
        WHEN AVG(complexity_score) > 7 THEN 'High complexity query that may benefit from simplification'
        WHEN AVG(duration_ms) > 300000 THEN 'Long running query that needs performance optimization'
        WHEN AVG(compute_cost_dbu) > 20 THEN 'High cost query that needs cost optimization'
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
    -- Optimization priority using your SLOW/MODERATE/FAST logic
    CASE 
        WHEN AVG(duration_ms) > 300000 THEN 'HIGH'     -- SLOW queries
        WHEN AVG(duration_ms) > 60000 THEN 'MEDIUM'    -- MODERATE queries  
        ELSE 'LOW'                                      -- FAST queries
    END as optimization_priority,
    -- Optimization recommendations
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'Replace SELECT * with specific column names'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'Add LIMIT clause to ORDER BY queries'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'Add proper JOIN conditions'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'Add partition filters to WHERE clause'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'Remove redundant DISTINCT'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'Use UNION ALL when appropriate'
        WHEN AVG(complexity_score) > 7 THEN 'Consider breaking down complex query into simpler parts'
        WHEN AVG(duration_ms) > 300000 THEN 'Review query execution plan and consider indexing'
        WHEN AVG(compute_cost_dbu) > 20 THEN 'Optimize data access patterns and consider caching'
        ELSE 'Review for general optimization opportunities'
    END as optimization_recommendations,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM query_performance_raw
WHERE execution_status = 'FINISHED'
GROUP BY query_hash, query_text, query_type
HAVING COUNT(*) >= 2;  -- Only patterns that occur more than once

-- =============================================================================
-- Initialize optimization_tracking (empty for now)
-- =============================================================================

-- Table exists but starts empty - will be populated when optimizations are implemented

-- =============================================================================
-- Initialize performance_baselines (calculated from historical data)
-- =============================================================================

INSERT OVERWRITE performance_baselines
SELECT 
    UUID() as baseline_id,
    query_hash,
    workspace_id,
    user_id,
    CURRENT_DATE() - INTERVAL 30 DAYS as baseline_period_start,
    CURRENT_DATE() - INTERVAL 1 DAY as baseline_period_end,
    AVG(duration_ms) as baseline_avg_duration_ms,
    percentile_approx(duration_ms, 0.95) as baseline_p95_duration_ms,
    AVG(compute_cost_dbu) as baseline_avg_cost_dbu,
    CAST(SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as baseline_success_rate,
    COUNT(*) as baseline_execution_count,
    -- Set thresholds at 2x the 95th percentile
    percentile_approx(duration_ms, 0.95) * 2 as threshold_duration_ms,
    percentile_approx(compute_cost_dbu, 0.95) * 2 as threshold_cost_dbu,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM query_performance_raw
WHERE execution_status IN ('FINISHED', 'FAILED')
GROUP BY query_hash, workspace_id, user_id
HAVING COUNT(*) >= 5;  -- Minimum executions for baseline

-- =============================================================================
-- Verify Data Load
-- =============================================================================

SELECT 'Core Tables Population Summary:' as status;

SELECT 
    'query_performance_raw' as table_name,
    COUNT(*) as row_count,
    MIN(start_time) as earliest_data,
    MAX(start_time) as latest_data
FROM query_performance_raw
UNION ALL
SELECT 
    'query_patterns' as table_name,
    COUNT(*) as row_count,
    MIN(first_seen) as earliest_data,
    MAX(last_seen) as latest_data
FROM query_patterns
UNION ALL
SELECT 
    'optimization_tracking' as table_name,
    COUNT(*) as row_count,
    NULL as earliest_data,
    NULL as latest_data
FROM optimization_tracking
UNION ALL
SELECT 
    'performance_baselines' as table_name,
    COUNT(*) as row_count,
    MIN(baseline_period_start) as earliest_data,
    MAX(baseline_period_end) as latest_data
FROM performance_baselines;