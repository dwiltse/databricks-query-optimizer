-- Trusted Asset: Performance Categorization Function
-- This function implements your established business logic for query performance categorization
-- and can be used as a trusted asset across all Genie Spaces for consistent analysis

CREATE OR REPLACE FUNCTION mcp.query_optimization.categorize_query_performance (
    execution_duration_ms BIGINT,
    result_fetch_duration_ms BIGINT DEFAULT NULL,
    error_message STRING DEFAULT NULL,
    read_bytes BIGINT DEFAULT NULL,
    read_rows BIGINT DEFAULT NULL
) 
RETURNS STRUCT<
    performance_category: STRING,
    optimization_flag: STRING,
    bytes_per_row_efficiency: DOUBLE,
    priority_score: INT
>
LANGUAGE SQL
COMMENT 'Categorizes query performance using established business thresholds: SLOW (>300s), MODERATE (60-300s), FAST (<60s). Also calculates optimization flags and efficiency metrics for consistent analysis across all Genie Spaces.'
AS
$$
    SELECT STRUCT(
        -- Performance categorization based on your business logic
        CASE
            WHEN execution_duration_ms > 300000 THEN 'SLOW'
            WHEN execution_duration_ms > 60000 THEN 'MODERATE'
            ELSE 'FAST'
        END AS performance_category,
        
        -- Optimization flags based on your business logic
        CASE
            WHEN error_message IS NOT NULL THEN 'ERROR'
            WHEN result_fetch_duration_ms > 30000 THEN 'SLOW_FETCH'
            ELSE 'HEALTHY'
        END AS optimization_flag,
        
        -- Efficiency metric based on your business logic
        CASE
            WHEN read_bytes > 0 AND read_rows > 0 THEN read_bytes / read_rows
            ELSE NULL
        END AS bytes_per_row_efficiency,
        
        -- Priority score for optimization (1=highest, 10=lowest priority)
        CASE
            WHEN error_message IS NOT NULL THEN 1  -- Critical: Failed queries
            WHEN execution_duration_ms > 300000 AND result_fetch_duration_ms > 30000 THEN 2  -- Critical: Slow execution + slow fetch
            WHEN execution_duration_ms > 300000 THEN 3  -- High: Slow execution
            WHEN result_fetch_duration_ms > 30000 THEN 4  -- High: Slow fetch
            WHEN execution_duration_ms > 60000 THEN 5  -- Medium: Moderate execution
            WHEN read_bytes > 0 AND read_rows > 0 AND (read_bytes / read_rows) > 100000 THEN 6  -- Medium: Inefficient data scan
            WHEN read_bytes > 0 AND read_rows > 0 AND (read_bytes / read_rows) > 10000 THEN 7  -- Low: Concerning efficiency
            ELSE 8  -- Low: Healthy queries
        END AS priority_score
    )
$$;

-- Example usage for Genie Spaces:
-- SELECT 
--     statement_id,
--     categorize_query_performance(execution_duration_ms, result_fetch_duration_ms, error_message, read_bytes, read_rows) as performance_analysis
-- FROM system.query.history 
-- WHERE end_time >= current_date() - INTERVAL 1 DAY;

-- Grant permissions for Genie Space access
GRANT EXECUTE ON FUNCTION mcp.query_optimization.categorize_query_performance TO `genie-space-users`;