-- Top 3 Worst Queries Analysis for POC Demo
-- This script identifies the worst performing queries with actionable optimization recommendations
-- PREREQUISITES: Run 01_schema_setup.sql, 02_core_tables.sql, and populate_core_tables.sql first

USE mcp.query_optimization;

-- =============================================================================
-- Prerequisite Check: Ensure Required Tables Exist and Have Data
-- =============================================================================

SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'ERROR: query_performance_raw table is empty. Run populate_core_tables.sql first.'
        ELSE CONCAT('SUCCESS: Found ', COUNT(*), ' records in query_performance_raw table')
    END AS prerequisite_check
FROM query_performance_raw;

-- =============================================================================
-- Top 3 Worst Queries by Performance Impact Score
-- =============================================================================

WITH query_performance_scored AS (
    SELECT 
        statement_id,
        statement_text,
        executed_by,
        execution_duration_ms,
        read_bytes,
        read_rows,
        end_time,
        workspace_id,
        
        -- Calculate performance impact score (0-100, higher = worse)
        LEAST(100, GREATEST(0,
            -- Duration impact (0-40 points)
            (execution_duration_ms / 1000.0 / 300.0) * 40 + -- 40 points for 5+ minute queries
            
            -- Data inefficiency impact (0-30 points) 
            CASE 
                WHEN read_rows > 0 THEN LEAST(30, (read_bytes / read_rows / 10000.0) * 30)
                ELSE 0 
            END +
            
            -- Anti-pattern penalties (0-30 points)
            (CASE WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 10 ELSE 0 END) +
            (CASE WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 10 ELSE 0 END) +
            (CASE WHEN UPPER(statement_text) LIKE '% FROM %,%' AND UPPER(statement_text) NOT LIKE '%JOIN%' THEN 10 ELSE 0 END)
        )) AS performance_impact_score,
        
        -- Estimated monthly cost impact (rough DBU estimation)
        (execution_duration_ms / 3600000.0) * 2.5 * 30 AS estimated_monthly_dbu_cost,
        
        -- Optimization category
        CASE 
            WHEN execution_duration_ms > 300000 THEN 'SLOW_EXECUTION'
            WHEN read_rows > 0 AND (read_bytes / read_rows) > 100000 THEN 'DATA_INEFFICIENT' 
            WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 'SELECT_ALL_ANTIPATTERN'
            WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
            WHEN UPPER(statement_text) LIKE '% FROM %,%' AND UPPER(statement_text) NOT LIKE '%JOIN%' THEN 'CARTESIAN_JOIN'
            ELSE 'GENERAL_PERFORMANCE'
        END AS optimization_category,
        
        -- Actionable recommendations
        CASE 
            WHEN execution_duration_ms > 300000 AND UPPER(statement_text) LIKE '%SELECT *%' THEN 
                'Replace SELECT * with specific columns and review execution plan for indexing opportunities'
            WHEN execution_duration_ms > 300000 THEN 
                'Review query execution plan, consider indexing, and check for unnecessary data processing'
            WHEN read_rows > 0 AND (read_bytes / read_rows) > 100000 THEN 
                'Optimize data access patterns - query is reading too many bytes per row'
            WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 
                'Replace SELECT * with specific column names to reduce data transfer'
            WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 
                'Add LIMIT clause to ORDER BY queries to prevent full dataset sorting'
            WHEN UPPER(statement_text) LIKE '% FROM %,%' AND UPPER(statement_text) NOT LIKE '%JOIN%' THEN 
                'Replace cartesian join with proper JOIN conditions'
            ELSE 
                'Consider query optimization techniques like predicate pushdown and column pruning'
        END AS optimization_recommendation,
        
        -- Estimated savings potential (percentage)
        CASE 
            WHEN UPPER(statement_text) LIKE '%SELECT *%' THEN 40
            WHEN UPPER(statement_text) LIKE '%ORDER BY%' AND UPPER(statement_text) NOT LIKE '%LIMIT%' THEN 60
            WHEN UPPER(statement_text) LIKE '% FROM %,%' AND UPPER(statement_text) NOT LIKE '%JOIN%' THEN 80
            WHEN read_rows > 0 AND (read_bytes / read_rows) > 100000 THEN 45
            WHEN execution_duration_ms > 600000 THEN 35
            WHEN execution_duration_ms > 300000 THEN 25
            ELSE 15
        END AS estimated_savings_percentage

    FROM query_performance_raw
    WHERE execution_status = 'FINISHED'
        AND execution_duration_ms > 0
        AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
)

SELECT 
    'Query Rank: #' || ROW_NUMBER() OVER (ORDER BY performance_impact_score DESC) AS query_rank,
    statement_id,
    LEFT(statement_text, 200) || '...' AS statement_preview,
    executed_by AS user_email,
    
    -- Performance metrics
    ROUND(execution_duration_ms / 1000.0, 2) AS duration_seconds,
    ROUND(read_bytes / 1024.0 / 1024.0, 2) AS data_read_mb,
    read_rows,
    ROUND(performance_impact_score, 1) AS impact_score,
    
    -- Financial impact
    ROUND(estimated_monthly_dbu_cost, 2) AS monthly_dbu_cost,
    ROUND(estimated_monthly_dbu_cost * estimated_savings_percentage / 100.0, 2) AS potential_monthly_savings_dbu,
    
    -- Optimization details
    optimization_category,
    optimization_recommendation,
    estimated_savings_percentage || '% potential improvement' AS savings_potential,
    
    -- Deployment steps
    CASE 
        WHEN optimization_category = 'SELECT_ALL_ANTIPATTERN' THEN 
            '1. Identify specific columns needed 2. Replace SELECT * with column list 3. Test performance 4. Deploy'
        WHEN optimization_category = 'UNBOUNDED_SORT' THEN 
            '1. Determine appropriate LIMIT value 2. Add LIMIT clause 3. Validate results 4. Deploy'
        WHEN optimization_category = 'CARTESIAN_JOIN' THEN 
            '1. Identify join condition 2. Add proper JOIN syntax 3. Validate logic 4. Deploy'
        WHEN optimization_category = 'DATA_INEFFICIENT' THEN 
            '1. Review column selection 2. Add WHERE filters 3. Consider data partitioning 4. Deploy'
        ELSE 
            '1. Run EXPLAIN PLAN 2. Identify bottlenecks 3. Apply optimizations 4. Test and deploy'
    END AS deployment_steps,
    
    end_time,
    workspace_id

FROM query_performance_scored
WHERE performance_impact_score > 10  -- Only show queries with meaningful impact
ORDER BY performance_impact_score DESC
LIMIT 3;

-- =============================================================================
-- Summary Statistics for POC Demo
-- =============================================================================

SELECT 
    COUNT(*) AS total_queries_analyzed,
    COUNT(CASE WHEN performance_impact_score > 50 THEN 1 END) AS critical_queries,
    COUNT(CASE WHEN performance_impact_score > 30 THEN 1 END) AS high_impact_queries,
    ROUND(AVG(estimated_monthly_dbu_cost), 2) AS avg_monthly_cost_per_query,
    ROUND(SUM(estimated_monthly_dbu_cost * estimated_savings_percentage / 100.0), 2) AS total_potential_monthly_savings,
    ROUND(AVG(estimated_savings_percentage), 1) AS avg_optimization_potential_pct
FROM query_performance_scored
WHERE performance_impact_score > 10;

-- =============================================================================
-- Optimization Categories Breakdown
-- =============================================================================

SELECT 
    optimization_category,
    COUNT(*) AS query_count,
    ROUND(AVG(performance_impact_score), 1) AS avg_impact_score,
    ROUND(SUM(estimated_monthly_dbu_cost * estimated_savings_percentage / 100.0), 2) AS category_savings_potential,
    ROUND(AVG(estimated_savings_percentage), 1) AS avg_improvement_potential_pct
FROM query_performance_scored
WHERE performance_impact_score > 10
GROUP BY optimization_category
ORDER BY category_savings_potential DESC;