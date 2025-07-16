-- Query Performance Overview
-- Dashboard query for performance monitoring and trend analysis

-- Query 1: Performance Overview Summary
SELECT 
    'Performance Overview' as dashboard_section,
    COUNT(*) as total_queries,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(DISTINCT workspace_id) as active_workspaces,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) as successful_queries,
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries,
    ROUND(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate_pct,
    -- Performance categories
    COUNT(CASE WHEN duration_ms < 10000 THEN 1 END) as fast_queries,
    COUNT(CASE WHEN duration_ms BETWEEN 10000 AND 60000 THEN 1 END) as medium_queries,
    COUNT(CASE WHEN duration_ms > 60000 THEN 1 END) as slow_queries,
    -- Optimization opportunity counts
    COUNT(CASE WHEN optimization_score < 5 THEN 1 END) as needs_optimization,
    COUNT(CASE WHEN optimization_score >= 8 THEN 1 END) as well_optimized,
    AVG(optimization_score) as avg_optimization_score
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS;

-- Query 2: Daily Performance Trends
SELECT 
    query_date,
    total_queries,
    unique_patterns,
    avg_duration_ms,
    p95_duration_ms,
    total_cost_dbu,
    success_rate,
    fast_queries,
    medium_queries,
    slow_queries,
    avg_optimization_score,
    -- Calculate day-over-day changes
    LAG(total_queries, 1) OVER (ORDER BY query_date) as prev_day_queries,
    LAG(avg_duration_ms, 1) OVER (ORDER BY query_date) as prev_day_avg_duration,
    LAG(total_cost_dbu, 1) OVER (ORDER BY query_date) as prev_day_cost,
    ROUND((total_queries - LAG(total_queries, 1) OVER (ORDER BY query_date)) * 100.0 / 
          NULLIF(LAG(total_queries, 1) OVER (ORDER BY query_date), 0), 2) as query_change_pct,
    ROUND((avg_duration_ms - LAG(avg_duration_ms, 1) OVER (ORDER BY query_date)) * 100.0 / 
          NULLIF(LAG(avg_duration_ms, 1) OVER (ORDER BY query_date), 0), 2) as duration_change_pct,
    ROUND((total_cost_dbu - LAG(total_cost_dbu, 1) OVER (ORDER BY query_date)) * 100.0 / 
          NULLIF(LAG(total_cost_dbu, 1) OVER (ORDER BY query_date), 0), 2) as cost_change_pct
FROM mcp.query_optimization.mv_daily_performance
WHERE query_date >= current_date() - INTERVAL 30 DAYS
ORDER BY query_date DESC;

-- Query 3: Hourly Performance Patterns
SELECT 
    query_hour,
    AVG(query_count) as avg_queries_per_hour,
    AVG(avg_duration_ms) as avg_duration_ms,
    AVG(total_cost_dbu) as avg_cost_dbu,
    AVG(success_rate) as avg_success_rate,
    -- Identify peak hours
    CASE 
        WHEN AVG(query_count) > (SELECT AVG(query_count) * 1.5 FROM mcp.query_optimization.mv_hourly_performance 
                                 WHERE query_date >= current_date() - INTERVAL 7 DAYS) THEN 'PEAK'
        WHEN AVG(query_count) < (SELECT AVG(query_count) * 0.5 FROM mcp.query_optimization.mv_hourly_performance 
                                 WHERE query_date >= current_date() - INTERVAL 7 DAYS) THEN 'LOW'
        ELSE 'NORMAL'
    END as usage_pattern
FROM mcp.query_optimization.mv_hourly_performance
WHERE query_date >= current_date() - INTERVAL 7 DAYS
GROUP BY query_hour
ORDER BY query_hour;

-- Query 4: Top Slow Queries
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    LEFT(query_text, 200) as query_preview,
    optimization_score,
    -- Calculate relative performance
    ROUND(duration_ms / (SELECT AVG(duration_ms) FROM mcp.query_optimization.query_performance_raw 
                        WHERE start_time >= current_date() - INTERVAL 7 DAYS), 2) as duration_vs_avg,
    ROUND(compute_cost_dbu / (SELECT AVG(compute_cost_dbu) FROM mcp.query_optimization.query_performance_raw 
                             WHERE start_time >= current_date() - INTERVAL 7 DAYS), 2) as cost_vs_avg,
    -- Rank by duration
    RANK() OVER (ORDER BY duration_ms DESC) as duration_rank
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
    AND duration_ms > 60000  -- Only queries longer than 1 minute
ORDER BY duration_ms DESC
LIMIT 20;

-- Query 5: Most Expensive Queries
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    LEFT(query_text, 200) as query_preview,
    optimization_score,
    -- Calculate efficiency metrics
    ROUND(duration_ms / 1000.0, 2) as duration_seconds,
    ROUND(compute_cost_dbu / (duration_ms / 1000.0), 4) as cost_per_second,
    ROUND(bytes_read / (1024 * 1024 * 1024.0), 2) as gb_read,
    -- Rank by cost
    RANK() OVER (ORDER BY compute_cost_dbu DESC) as cost_rank
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
    AND compute_cost_dbu > 5  -- Only queries with significant cost
ORDER BY compute_cost_dbu DESC
LIMIT 20;

-- Query 6: Failed Queries Analysis
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    error_message,
    LEFT(query_text, 200) as query_preview,
    -- Categorize failures
    CASE 
        WHEN error_message LIKE '%timeout%' OR error_message LIKE '%cancelled%' THEN 'TIMEOUT'
        WHEN error_message LIKE '%memory%' OR error_message LIKE '%OutOfMemory%' THEN 'MEMORY'
        WHEN error_message LIKE '%permission%' OR error_message LIKE '%access%' THEN 'PERMISSION'
        WHEN error_message LIKE '%syntax%' OR error_message LIKE '%parse%' THEN 'SYNTAX'
        WHEN error_message LIKE '%connection%' OR error_message LIKE '%network%' THEN 'CONNECTION'
        WHEN error_message LIKE '%resource%' OR error_message LIKE '%capacity%' THEN 'RESOURCE'
        ELSE 'OTHER'
    END as failure_category,
    -- Calculate failure frequency for this pattern
    COUNT(*) OVER (PARTITION BY query_hash) as pattern_failure_count
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FAILED'
ORDER BY start_time DESC
LIMIT 50;

-- Query 7: Workspace Performance Comparison
SELECT 
    workspace_id,
    COUNT(*) as total_queries,
    COUNT(DISTINCT user_id) as active_users,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    ROUND(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate_pct,
    AVG(optimization_score) as avg_optimization_score,
    -- Calculate relative performance vs. overall average
    ROUND(AVG(duration_ms) / (SELECT AVG(duration_ms) FROM mcp.query_optimization.query_performance_raw 
                              WHERE start_time >= current_date() - INTERVAL 7 DAYS), 2) as duration_vs_global_avg,
    ROUND(AVG(compute_cost_dbu) / (SELECT AVG(compute_cost_dbu) FROM mcp.query_optimization.query_performance_raw 
                                   WHERE start_time >= current_date() - INTERVAL 7 DAYS), 2) as cost_vs_global_avg,
    -- Rank workspaces by performance
    RANK() OVER (ORDER BY AVG(duration_ms) ASC) as performance_rank,
    RANK() OVER (ORDER BY AVG(compute_cost_dbu) ASC) as cost_efficiency_rank
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
GROUP BY workspace_id
ORDER BY total_cost_dbu DESC;

-- Query 8: Query Type Distribution
SELECT 
    query_type,
    COUNT(*) as query_count,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    AVG(bytes_read) as avg_bytes_read,
    ROUND(COUNT(CASE WHEN execution_status = 'FINISHED' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate_pct,
    AVG(optimization_score) as avg_optimization_score,
    -- Calculate percentage of total queries
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcp.query_optimization.query_performance_raw 
                              WHERE start_time >= current_date() - INTERVAL 7 DAYS), 2) as pct_of_total_queries
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY query_type
ORDER BY query_count DESC;

-- Query 9: Performance Alerts Summary
SELECT 
    alert_date,
    SUM(long_running_queries) as total_long_running,
    SUM(high_cost_queries) as total_high_cost,
    SUM(failed_queries) as total_failed,
    SUM(large_scan_queries) as total_large_scan,
    SUM(total_alerts) as total_alerts,
    AVG(avg_severity_score) as avg_severity,
    -- Calculate alert trends
    LAG(SUM(total_alerts), 1) OVER (ORDER BY alert_date) as prev_day_alerts,
    ROUND((SUM(total_alerts) - LAG(SUM(total_alerts), 1) OVER (ORDER BY alert_date)) * 100.0 / 
          NULLIF(LAG(SUM(total_alerts), 1) OVER (ORDER BY alert_date), 0), 2) as alert_change_pct
FROM mcp.query_optimization.mv_performance_alerts
WHERE alert_date >= current_date() - INTERVAL 14 DAYS
GROUP BY alert_date
ORDER BY alert_date DESC;

-- Query 10: Optimization Score Distribution
SELECT 
    CASE 
        WHEN optimization_score >= 9 THEN 'Excellent (9-10)'
        WHEN optimization_score >= 7 THEN 'Good (7-8)'
        WHEN optimization_score >= 5 THEN 'Average (5-6)'
        WHEN optimization_score >= 3 THEN 'Poor (3-4)'
        ELSE 'Critical (1-2)'
    END as optimization_category,
    COUNT(*) as query_count,
    AVG(duration_ms) as avg_duration_ms,
    AVG(compute_cost_dbu) as avg_cost_dbu,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mcp.query_optimization.query_performance_raw 
                              WHERE start_time >= current_date() - INTERVAL 7 DAYS), 2) as pct_of_total,
    -- Calculate potential savings if optimized to score 8+
    CASE 
        WHEN optimization_score < 8 THEN SUM(compute_cost_dbu) * 0.3
        ELSE 0
    END as potential_savings_dbu
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
GROUP BY 
    CASE 
        WHEN optimization_score >= 9 THEN 'Excellent (9-10)'
        WHEN optimization_score >= 7 THEN 'Good (7-8)'
        WHEN optimization_score >= 5 THEN 'Average (5-6)'
        WHEN optimization_score >= 3 THEN 'Poor (3-4)'
        ELSE 'Critical (1-2)'
    END,
    CASE WHEN optimization_score < 8 THEN 1 ELSE 0 END
ORDER BY 
    CASE 
        WHEN optimization_category = 'Excellent (9-10)' THEN 1
        WHEN optimization_category = 'Good (7-8)' THEN 2
        WHEN optimization_category = 'Average (5-6)' THEN 3
        WHEN optimization_category = 'Poor (3-4)' THEN 4
        ELSE 5
    END;