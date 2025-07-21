-- Trusted Asset: Cost Impact Analysis Function
-- This function calculates cost impact and optimization potential for queries
-- Used as a trusted asset for consistent cost analysis across Genie Spaces

CREATE OR REPLACE FUNCTION mcp.query_optimization.analyze_cost_impact (
    execution_duration_ms BIGINT,
    total_duration_ms BIGINT,
    dbu_consumed DECIMAL(10,4),
    read_bytes BIGINT,
    warehouse_type STRING DEFAULT 'SQL_COMPUTE'
) 
RETURNS STRUCT<
    cost_efficiency_score: INT,
    optimization_potential_percent: INT,
    estimated_monthly_cost_usd: DECIMAL(10,2),
    resource_waste_indicator: STRING,
    recommended_action: STRING
>
LANGUAGE SQL
COMMENT 'Analyzes query cost impact and optimization potential. Returns cost efficiency score (1-10), optimization potential percentage, estimated monthly costs, and recommended actions for consistent cost analysis across Genie Spaces.'
AS
$$
    SELECT STRUCT(
        -- Cost efficiency score (1=very inefficient, 10=very efficient)
        CASE
            WHEN execution_duration_ms IS NULL OR dbu_consumed IS NULL THEN NULL
            WHEN execution_duration_ms > 300000 AND dbu_consumed > 10 THEN 1  -- Very inefficient: slow + expensive
            WHEN execution_duration_ms > 300000 AND dbu_consumed > 5 THEN 2   -- Poor: slow + moderately expensive
            WHEN execution_duration_ms > 300000 THEN 3                        -- Poor: slow execution
            WHEN dbu_consumed > 10 THEN 4                                     -- Poor: high cost
            WHEN execution_duration_ms > 60000 AND dbu_consumed > 2 THEN 5    -- Average: moderate time + cost
            WHEN execution_duration_ms > 60000 THEN 6                         -- Average: moderate time
            WHEN dbu_consumed > 2 THEN 7                                      -- Good: low time, moderate cost
            WHEN execution_duration_ms < 30000 AND dbu_consumed < 1 THEN 10   -- Excellent: fast + cheap
            WHEN execution_duration_ms < 60000 AND dbu_consumed < 2 THEN 9    -- Very good: fast + low cost
            ELSE 8                                                            -- Good: balanced performance
        END AS cost_efficiency_score,
        
        -- Optimization potential percentage
        CASE
            WHEN execution_duration_ms IS NULL THEN 0
            WHEN execution_duration_ms > 600000 THEN 70  -- Very slow queries: high optimization potential
            WHEN execution_duration_ms > 300000 THEN 50  -- Slow queries: moderate-high potential
            WHEN execution_duration_ms > 120000 THEN 30  -- Moderately slow: moderate potential
            WHEN execution_duration_ms > 60000 THEN 15   -- Slightly slow: low-moderate potential
            WHEN read_bytes > 1000000000 AND execution_duration_ms < 60000 THEN 25  -- Fast but data-heavy: data optimization potential
            ELSE 5  -- Fast queries: minimal potential
        END AS optimization_potential_percent,
        
        -- Estimated monthly cost (assuming daily execution)
        CASE 
            WHEN dbu_consumed IS NULL THEN NULL
            WHEN warehouse_type = 'SQL_COMPUTE' THEN ROUND(dbu_consumed * 0.22 * 30, 2)  -- SQL Warehouse DBU rate
            WHEN warehouse_type = 'JOBS_COMPUTE' THEN ROUND(dbu_consumed * 0.15 * 30, 2)  -- Jobs compute DBU rate
            ELSE ROUND(dbu_consumed * 0.20 * 30, 2)  -- Default DBU rate
        END AS estimated_monthly_cost_usd,
        
        -- Resource waste indicator
        CASE
            WHEN total_duration_ms IS NULL OR execution_duration_ms IS NULL THEN 'UNKNOWN'
            WHEN (total_duration_ms - execution_duration_ms) > execution_duration_ms THEN 'HIGH_WAIT_TIME'  -- More waiting than executing
            WHEN (total_duration_ms - execution_duration_ms) > (execution_duration_ms * 0.5) THEN 'MODERATE_WAIT_TIME'  -- Significant waiting
            WHEN read_bytes > 0 AND execution_duration_ms > 0 AND (read_bytes / (execution_duration_ms / 1000)) < 1000000 THEN 'LOW_THROUGHPUT'  -- <1MB/second
            WHEN dbu_consumed > 0 AND execution_duration_ms > 0 AND (dbu_consumed / (execution_duration_ms / 3600000)) > 2 THEN 'HIGH_DBU_RATE'  -- >2 DBU per hour
            ELSE 'EFFICIENT'
        END AS resource_waste_indicator,
        
        -- Recommended action based on analysis
        CASE
            WHEN execution_duration_ms > 600000 THEN 'CRITICAL: Immediate optimization required - query takes >10 minutes'
            WHEN execution_duration_ms > 300000 AND dbu_consumed > 10 THEN 'HIGH: Optimize for both performance and cost - slow and expensive'
            WHEN execution_duration_ms > 300000 THEN 'HIGH: Focus on performance optimization - query is slow'
            WHEN dbu_consumed > 10 THEN 'HIGH: Focus on cost optimization - query is expensive'
            WHEN execution_duration_ms > 60000 AND dbu_consumed > 2 THEN 'MEDIUM: Consider optimization - moderate performance/cost impact'
            WHEN read_bytes > 1000000000 THEN 'MEDIUM: Review data access patterns - processing large data volumes'
            WHEN (total_duration_ms - execution_duration_ms) > (execution_duration_ms * 0.5) THEN 'MEDIUM: Address compute provisioning - high wait times'
            ELSE 'LOW: Monitor for changes - currently acceptable performance'
        END AS recommended_action
    )
$$;

-- Example usage for Genie Spaces:
-- SELECT 
--     statement_id,
--     analyze_cost_impact(execution_duration_ms, total_duration_ms, dbu_consumed, read_bytes, 'SQL_COMPUTE') as cost_analysis
-- FROM system.query.history 
-- WHERE end_time >= current_date() - INTERVAL 7 DAY;

-- Grant permissions for Genie Space access
GRANT EXECUTE ON FUNCTION mcp.query_optimization.analyze_cost_impact TO `genie-space-users`;