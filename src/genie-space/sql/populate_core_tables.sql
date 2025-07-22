-- Populate Core Tables - Simple ETL for POC Testing
-- This script populates the core tables (Step 2) from system tables
-- Run this BEFORE running the refresh_performance_tables.py notebook

USE mcp.query_optimization;

-- =============================================================================
-- Populate query_performance_raw (from system.query.history)
-- =============================================================================

INSERT OVERWRITE query_performance_raw
SELECT 
    statement_id,
    workspace_id,
    executed_by_user_id as user_id,
    executed_by,
    statement_text,
    -- Calculate query hash inline (no functions in Databricks SQL)
    SHA2(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        UPPER(TRIM(statement_text)),
                        '[0-9]+', 'N'  -- Replace numbers with N
                    ),
                    '\'[^\']*\'', 'S'  -- Replace string literals with S
                ),
                '\\s+', ' '  -- Normalize whitespace
            ),
            '--[^\n]*', ' '  -- Remove comments
        ),
        256
    ) as query_hash,
    start_time,  -- Actual field exists!
    end_time,
    total_duration_ms as execution_duration_ms,
    read_rows,
    read_bytes,
    0 as rows_produced,  -- Not available in system.query.history
    -- Approximate compute cost (basic estimation)
    CAST(total_duration_ms AS DOUBLE) / 3600000 * 2.5 as compute_cost_dbu,
    execution_status,
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
    -- Calculate complexity score inline (1-10)
    LEAST(10, GREATEST(1, 
        1 + 
        ((LENGTH(statement_text) - LENGTH(REPLACE(UPPER(statement_text), 'SELECT', ''))) / 6) * 0.5 +
        ((LENGTH(statement_text) - LENGTH(REPLACE(UPPER(statement_text), 'JOIN', ''))) / 4) * 1.0 +
        ((LENGTH(statement_text) - LENGTH(REPLACE(UPPER(statement_text), 'WHERE', ''))) / 5) * 0.3 +
        ((LENGTH(statement_text) - LENGTH(REPLACE(UPPER(statement_text), 'GROUP BY', ''))) / 8) * 0.8 +
        ((LENGTH(statement_text) - LENGTH(REPLACE(UPPER(statement_text), 'ORDER BY', ''))) / 8) * 0.6 +
        (LENGTH(statement_text) / 1000) * 0.1
    )) as complexity_score,
    -- Calculate optimization score inline (1-10, higher is better)
    LEAST(10, GREATEST(1,
        10 -
        -- Penalty for inefficient patterns
        (CASE WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 2 ELSE 0 END) -
        (CASE WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 3 ELSE 0 END) -
        (CASE WHEN UPPER(statement_text) LIKE '%JOIN%' AND UPPER(statement_text) NOT LIKE '%ON%' THEN 4 ELSE 0 END) -
        (CASE WHEN UPPER(statement_text) LIKE '%DISTINCT%' AND UPPER(statement_text) LIKE '%GROUP BY%' THEN 1 ELSE 0 END) -
        -- Penalty for performance issues
        (CASE WHEN total_duration_ms > 300000 THEN 2 ELSE 0 END) -
        (CASE WHEN read_bytes > 5368709120 THEN 1 ELSE 0 END) -
        -- Penalty for very long queries
        (CASE WHEN LENGTH(statement_text) > 10000 THEN 1 ELSE 0 END)
    )) as optimization_score,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM system.query.history
WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
    AND statement_text IS NOT NULL
    AND total_duration_ms IS NOT NULL
    AND execution_status = 'FINISHED';

-- =============================================================================
-- Populate query_patterns (from processed raw data)
-- =============================================================================

INSERT OVERWRITE query_patterns
SELECT 
    UUID() as pattern_id,
    query_hash,
    -- Determine pattern type based on query characteristics
    CASE 
        WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 'SELECT_ALL'
        WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
        WHEN UPPER(statement_text) LIKE '%JOIN%' AND UPPER(statement_text) NOT LIKE '%ON%' THEN 'CARTESIAN_JOIN'
        WHEN UPPER(statement_text) LIKE '%WHERE%' AND UPPER(statement_text) NOT LIKE '%PARTITION%' THEN 'UNPARTITIONED_FILTER'
        WHEN UPPER(statement_text) LIKE '%DISTINCT%' AND UPPER(statement_text) LIKE '%GROUP BY%' THEN 'REDUNDANT_DISTINCT'
        WHEN UPPER(statement_text) LIKE '%UNION%' AND UPPER(statement_text) NOT LIKE '%UNION ALL%' THEN 'UNION_OPTIMIZATION'
        WHEN AVG(complexity_score) > 7 THEN 'HIGH_COMPLEXITY'
        WHEN AVG(execution_duration_ms) > 300000 THEN 'LONG_RUNNING'
        WHEN AVG(compute_cost_dbu) > 20 THEN 'HIGH_COST'
        ELSE 'STANDARD'
    END as pattern_type,
    -- Pattern description
    CASE 
        WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 'Query uses SELECT * which may retrieve unnecessary columns'
        WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 'Query uses ORDER BY without LIMIT, sorting entire dataset'
        WHEN UPPER(statement_text) LIKE '%JOIN%' AND UPPER(statement_text) NOT LIKE '%ON%' THEN 'Query may have Cartesian JOIN without proper conditions'
        WHEN UPPER(statement_text) LIKE '%WHERE%' AND UPPER(statement_text) NOT LIKE '%PARTITION%' THEN 'Query filters may not utilize partitioning'
        WHEN UPPER(statement_text) LIKE '%DISTINCT%' AND UPPER(statement_text) LIKE '%GROUP BY%' THEN 'Query has redundant DISTINCT with GROUP BY'
        WHEN UPPER(statement_text) LIKE '%UNION%' AND UPPER(statement_text) NOT LIKE '%UNION ALL%' THEN 'Query uses UNION instead of UNION ALL'
        WHEN AVG(complexity_score) > 7 THEN 'High complexity query that may benefit from simplification'
        WHEN AVG(execution_duration_ms) > 300000 THEN 'Long running query that needs performance optimization'
        WHEN AVG(compute_cost_dbu) > 20 THEN 'High cost query that needs cost optimization'
        ELSE 'Standard query pattern'
    END as pattern_description,
    -- Create query template by replacing literals
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(statement_text, '[0-9]+', '?'),
            '\'[^\']*\'', '?'
        ),
        '"[^"]*"', '?'
    ) as query_template,
    MIN(start_time) as first_seen,
    MAX(start_time) as last_seen,
    COUNT(*) as occurrence_count,
    AVG(execution_duration_ms) as avg_duration_ms,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    -- Optimization priority using your SLOW/MODERATE/FAST logic
    CASE 
        WHEN AVG(execution_duration_ms) > 300000 THEN 'HIGH'     -- SLOW queries
        WHEN AVG(execution_duration_ms) > 60000 THEN 'MEDIUM'    -- MODERATE queries  
        ELSE 'LOW'                                      -- FAST queries
    END as optimization_priority,
    -- Optimization recommendations
    CASE 
        WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 'Replace SELECT * with specific column names'
        WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 'Add LIMIT clause to ORDER BY queries'
        WHEN UPPER(statement_text) LIKE '%JOIN%' AND UPPER(statement_text) NOT LIKE '%ON%' THEN 'Add proper JOIN conditions'
        WHEN UPPER(statement_text) LIKE '%WHERE%' AND UPPER(statement_text) NOT LIKE '%PARTITION%' THEN 'Add partition filters to WHERE clause'
        WHEN UPPER(statement_text) LIKE '%DISTINCT%' AND UPPER(statement_text) LIKE '%GROUP BY%' THEN 'Remove redundant DISTINCT'
        WHEN UPPER(statement_text) LIKE '%UNION%' AND UPPER(statement_text) NOT LIKE '%UNION ALL%' THEN 'Use UNION ALL when appropriate'
        WHEN AVG(complexity_score) > 7 THEN 'Consider breaking down complex query into simpler parts'
        WHEN AVG(execution_duration_ms) > 300000 THEN 'Review query execution plan and consider indexing'
        WHEN AVG(compute_cost_dbu) > 20 THEN 'Optimize data access patterns and consider caching'
        ELSE 'Review for general optimization opportunities'
    END as optimization_recommendations,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM query_performance_raw
WHERE execution_status = 'FINISHED'
GROUP BY query_hash, statement_text, query_type
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
    AVG(execution_duration_ms) as baseline_avg_duration_ms,
    percentile_approx(execution_duration_ms, 0.95) as baseline_p95_duration_ms,
    AVG(compute_cost_dbu) as baseline_avg_cost_dbu,
    CAST(SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as baseline_success_rate,
    COUNT(*) as baseline_execution_count,
    -- Set thresholds at 2x the 95th percentile
    percentile_approx(execution_duration_ms, 0.95) * 2 as threshold_duration_ms,
    percentile_approx(compute_cost_dbu, 0.95) * 2 as threshold_cost_dbu,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
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