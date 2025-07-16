-- Materialized Views for Fast Dashboard Performance
-- These views provide pre-computed aggregations for sub-second dashboard response times

USE mcp.query_optimization;

-- Hourly performance metrics for trend analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_performance
COMMENT 'Hourly query performance metrics for trend analysis'
AS
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
    AVG(optimization_score) as avg_optimization_score
FROM query_performance_raw
WHERE start_time >= current_date() - INTERVAL 90 DAYS
GROUP BY DATE(start_time), HOUR(start_time), workspace_id, user_id;

-- Daily performance summary for executive reporting
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_performance
COMMENT 'Daily query performance summary for executive reporting'
AS
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
    -- Performance categories
    COUNT(CASE WHEN duration_ms < 10000 THEN 1 END) as fast_queries,
    COUNT(CASE WHEN duration_ms BETWEEN 10000 AND 60000 THEN 1 END) as medium_queries,
    COUNT(CASE WHEN duration_ms > 60000 THEN 1 END) as slow_queries,
    -- Cost categories
    COUNT(CASE WHEN compute_cost_dbu < 1 THEN 1 END) as low_cost_queries,
    COUNT(CASE WHEN compute_cost_dbu BETWEEN 1 AND 10 THEN 1 END) as medium_cost_queries,
    COUNT(CASE WHEN compute_cost_dbu > 10 THEN 1 END) as high_cost_queries,
    -- Efficiency metrics
    SUM(compute_cost_dbu) / NULLIF(SUM(duration_ms), 0) * 1000 as cost_per_second,
    SUM(bytes_read) / NULLIF(SUM(duration_ms), 0) * 1000 as bytes_per_second,
    AVG(optimization_score) as avg_optimization_score
FROM query_performance_raw
WHERE start_time >= current_date() - INTERVAL 90 DAYS
GROUP BY DATE(start_time), workspace_id, user_id;

-- Query pattern performance for optimization prioritization
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pattern_performance
COMMENT 'Query pattern performance metrics for optimization prioritization'
AS
SELECT 
    qpr.query_hash,
    qp.pattern_type,
    qp.pattern_description,
    qpr.workspace_id,
    COUNT(*) as execution_count,
    COUNT(DISTINCT qpr.user_id) as unique_users,
    AVG(qpr.duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qpr.duration_ms) as p95_duration_ms,
    SUM(qpr.compute_cost_dbu) as total_cost_dbu,
    AVG(qpr.compute_cost_dbu) as avg_cost_dbu,
    SUM(qpr.bytes_read) as total_bytes_read,
    AVG(qpr.bytes_read) as avg_bytes_read,
    COUNT(CASE WHEN qpr.execution_status = 'FINISHED' THEN 1 END) as successful_executions,
    COUNT(CASE WHEN qpr.execution_status = 'FAILED' THEN 1 END) as failed_executions,
    CAST(COUNT(CASE WHEN qpr.execution_status = 'FINISHED' THEN 1 END) AS DECIMAL(10,4)) / COUNT(*) as success_rate,
    AVG(qpr.complexity_score) as avg_complexity_score,
    AVG(qpr.optimization_score) as avg_optimization_score,
    qp.optimization_priority,
    qp.optimization_recommendations,
    -- Calculate potential savings based on pattern efficiency
    CASE 
        WHEN AVG(qpr.duration_ms) > 60000 AND COUNT(*) > 10 THEN 'HIGH'
        WHEN AVG(qpr.duration_ms) > 30000 AND COUNT(*) > 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END as optimization_impact,
    -- Estimated monthly savings if optimized by 30%
    (SUM(qpr.compute_cost_dbu) * 0.3 * 30) as estimated_monthly_savings_dbu,
    MIN(qpr.start_time) as first_seen,
    MAX(qpr.start_time) as last_seen
FROM query_performance_raw qpr
JOIN query_patterns qp ON qpr.query_hash = qp.query_hash
WHERE qpr.start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY qpr.query_hash, qp.pattern_type, qp.pattern_description, qpr.workspace_id, 
         qp.optimization_priority, qp.optimization_recommendations;

-- User performance summary for individual analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_performance
COMMENT 'User performance summary for individual analysis and coaching'
AS
SELECT 
    user_id,
    user_email,
    workspace_id,
    COUNT(*) as total_queries,
    COUNT(DISTINCT query_hash) as unique_patterns,
    COUNT(DISTINCT DATE(start_time)) as active_days,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    SUM(bytes_read) as total_bytes_read,
    COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) as successful_queries,
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries,
    CAST(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) AS DECIMAL(10,4)) / COUNT(*) as success_rate,
    -- Performance distribution
    COUNT(CASE WHEN duration_ms < 10000 THEN 1 END) as fast_queries,
    COUNT(CASE WHEN duration_ms BETWEEN 10000 AND 60000 THEN 1 END) as medium_queries,
    COUNT(CASE WHEN duration_ms > 60000 THEN 1 END) as slow_queries,
    -- Efficiency metrics
    AVG(optimization_score) as avg_optimization_score,
    SUM(compute_cost_dbu) / NULLIF(COUNT(*), 0) as cost_per_query,
    SUM(duration_ms) / NULLIF(COUNT(*), 0) as avg_duration_per_query,
    -- Optimization opportunity score (1-10)
    CASE 
        WHEN AVG(optimization_score) < 3 THEN 9
        WHEN AVG(optimization_score) < 5 THEN 7
        WHEN AVG(optimization_score) < 7 THEN 5
        WHEN AVG(optimization_score) < 8 THEN 3
        ELSE 1
    END as optimization_opportunity_score,
    MIN(start_time) as first_query,
    MAX(start_time) as last_query
FROM query_performance_raw
WHERE start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY user_id, user_email, workspace_id;

-- Real-time performance alerts summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_performance_alerts
COMMENT 'Performance alerts summary for real-time monitoring'
AS
SELECT 
    DATE(start_time) as alert_date,
    HOUR(start_time) as alert_hour,
    workspace_id,
    user_id,
    -- Long running queries
    COUNT(CASE WHEN duration_ms > 300000 THEN 1 END) as long_running_queries,
    -- High cost queries
    COUNT(CASE WHEN compute_cost_dbu > 20 THEN 1 END) as high_cost_queries,
    -- Failed queries
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries,
    -- Large data scan queries
    COUNT(CASE WHEN bytes_read > 1073741824 THEN 1 END) as large_scan_queries,
    -- Total alert count
    COUNT(CASE WHEN duration_ms > 300000 OR compute_cost_dbu > 20 OR execution_status = 'FAILED' OR bytes_read > 1073741824 THEN 1 END) as total_alerts,
    -- Average severity (1-10 scale)
    AVG(
        CASE 
            WHEN duration_ms > 1800000 THEN 10  -- 30+ minutes
            WHEN duration_ms > 900000 THEN 8    -- 15+ minutes
            WHEN duration_ms > 300000 THEN 6    -- 5+ minutes
            WHEN compute_cost_dbu > 50 THEN 9
            WHEN compute_cost_dbu > 20 THEN 7
            WHEN execution_status = 'FAILED' THEN 8
            WHEN bytes_read > 5368709120 THEN 7  -- 5+ GB
            WHEN bytes_read > 1073741824 THEN 5  -- 1+ GB
            ELSE 1
        END
    ) as avg_severity_score
FROM query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY DATE(start_time), HOUR(start_time), workspace_id, user_id
HAVING COUNT(CASE WHEN duration_ms > 300000 OR compute_cost_dbu > 20 OR execution_status = 'FAILED' OR bytes_read > 1073741824 THEN 1 END) > 0;