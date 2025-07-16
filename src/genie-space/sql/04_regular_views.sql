-- Regular Views for Real-time Analysis and Alerting
-- These views provide fresh data access for real-time monitoring and flexible analysis

USE mcp.query_optimization;

-- Real-time slow query alerts
CREATE OR REPLACE VIEW v_current_slow_queries
COMMENT 'Real-time view of slow queries for immediate alerting'
AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    query_type,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    execution_status,
    LEFT(query_text, 200) as query_preview,
    'SLOW_QUERY' as alert_type,
    CASE 
        WHEN duration_ms > 1800000 THEN 'CRITICAL'  -- 30+ minutes
        WHEN duration_ms > 900000 THEN 'HIGH'       -- 15+ minutes
        WHEN duration_ms > 300000 THEN 'MEDIUM'     -- 5+ minutes
        ELSE 'LOW'
    END as severity,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    AND duration_ms > 300000  -- 5+ minutes
    AND execution_status = 'FINISHED'
ORDER BY duration_ms DESC;

-- Real-time expensive query alerts
CREATE OR REPLACE VIEW v_current_expensive_queries
COMMENT 'Real-time view of expensive queries for cost monitoring'
AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    query_type,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    execution_status,
    LEFT(query_text, 200) as query_preview,
    'EXPENSIVE_QUERY' as alert_type,
    CASE 
        WHEN compute_cost_dbu > 100 THEN 'CRITICAL'
        WHEN compute_cost_dbu > 50 THEN 'HIGH'
        WHEN compute_cost_dbu > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    AND compute_cost_dbu > 20
    AND execution_status = 'FINISHED'
ORDER BY compute_cost_dbu DESC;

-- Real-time query failures
CREATE OR REPLACE VIEW v_current_failed_queries
COMMENT 'Real-time view of failed queries for immediate attention'
AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    query_type,
    start_time,
    duration_ms,
    compute_cost_dbu,
    execution_status,
    error_message,
    LEFT(query_text, 200) as query_preview,
    'QUERY_FAILURE' as alert_type,
    CASE 
        WHEN error_message LIKE '%timeout%' THEN 'TIMEOUT'
        WHEN error_message LIKE '%memory%' THEN 'MEMORY'
        WHEN error_message LIKE '%permission%' THEN 'PERMISSION'
        WHEN error_message LIKE '%syntax%' THEN 'SYNTAX'
        ELSE 'OTHER'
    END as failure_category,
    'HIGH' as severity,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    AND execution_status = 'FAILED'
ORDER BY start_time DESC;

-- Optimization opportunities detection
CREATE OR REPLACE VIEW v_optimization_opportunities
COMMENT 'Real-time identification of optimization opportunities'
AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    query_text,
    -- Pattern-based optimization detection
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'SELECT_ALL_OPTIMIZATION'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'CARTESIAN_JOIN'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'UNPARTITIONED_FILTER'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'REDUNDANT_DISTINCT'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'UNION_OPTIMIZATION'
        WHEN bytes_read > 5368709120 AND duration_ms > 300000 THEN 'LARGE_SCAN_OPTIMIZATION'
        WHEN duration_ms > 300000 AND compute_cost_dbu > 10 THEN 'GENERAL_PERFORMANCE_OPTIMIZATION'
        ELSE 'OTHER'
    END as optimization_type,
    -- Priority scoring
    CASE 
        WHEN compute_cost_dbu > 50 AND duration_ms > 600000 THEN 'HIGH'
        WHEN compute_cost_dbu > 20 AND duration_ms > 300000 THEN 'MEDIUM'
        WHEN compute_cost_dbu > 10 OR duration_ms > 180000 THEN 'LOW'
        ELSE 'MINIMAL'
    END as optimization_priority,
    -- Specific recommendations
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'Replace SELECT * with specific column names to reduce data transfer'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'Add LIMIT clause to ORDER BY queries to prevent full dataset sorting'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'Add proper JOIN conditions to avoid Cartesian products'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'Add partition filters to WHERE clause for better performance'
        WHEN UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%' THEN 'Remove redundant DISTINCT when using GROUP BY'
        WHEN UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%' THEN 'Use UNION ALL instead of UNION when duplicates are acceptable'
        WHEN bytes_read > 5368709120 AND duration_ms > 300000 THEN 'Optimize data scanning by adding filters or improving table partitioning'
        WHEN duration_ms > 300000 AND compute_cost_dbu > 10 THEN 'Consider query restructuring or indexing for better performance'
        ELSE 'Review query structure for potential optimizations'
    END as recommendation,
    -- Estimated savings
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN compute_cost_dbu * 0.3
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN compute_cost_dbu * 0.5
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN compute_cost_dbu * 0.8
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN compute_cost_dbu * 0.4
        WHEN bytes_read > 5368709120 AND duration_ms > 300000 THEN compute_cost_dbu * 0.6
        ELSE compute_cost_dbu * 0.2
    END as estimated_savings_dbu,
    -- Implementation effort
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'LOW'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'LOW'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'HIGH'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'MEDIUM'
        WHEN bytes_read > 5368709120 AND duration_ms > 300000 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as implementation_effort
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
    AND (
        UPPER(query_text) LIKE '%SELECT *%' OR
        (UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%') OR
        (UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%') OR
        (UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%') OR
        (UPPER(query_text) LIKE '%DISTINCT%' AND UPPER(query_text) LIKE '%GROUP BY%') OR
        (UPPER(query_text) LIKE '%UNION%' AND UPPER(query_text) NOT LIKE '%UNION ALL%') OR
        (bytes_read > 5368709120 AND duration_ms > 300000) OR
        (duration_ms > 300000 AND compute_cost_dbu > 10)
    )
ORDER BY compute_cost_dbu DESC, duration_ms DESC;

-- Query performance comparison with baselines
CREATE OR REPLACE VIEW v_performance_anomalies
COMMENT 'Detect queries performing significantly worse than their baselines'
AS
SELECT 
    qh.query_id,
    qh.workspace_id,
    qh.user_id,
    qh.user_email,
    qh.start_time,
    qh.duration_ms,
    qh.compute_cost_dbu,
    qh.bytes_read,
    qh.execution_status,
    LEFT(qh.query_text, 200) as query_preview,
    pb.baseline_avg_duration_ms,
    pb.baseline_avg_cost_dbu,
    pb.threshold_duration_ms,
    pb.threshold_cost_dbu,
    -- Performance deviation
    (qh.duration_ms - pb.baseline_avg_duration_ms) as duration_deviation_ms,
    (qh.compute_cost_dbu - pb.baseline_avg_cost_dbu) as cost_deviation_dbu,
    ROUND((qh.duration_ms - pb.baseline_avg_duration_ms) / pb.baseline_avg_duration_ms * 100, 2) as duration_deviation_pct,
    ROUND((qh.compute_cost_dbu - pb.baseline_avg_cost_dbu) / pb.baseline_avg_cost_dbu * 100, 2) as cost_deviation_pct,
    -- Anomaly severity
    CASE 
        WHEN qh.duration_ms > pb.threshold_duration_ms * 2 THEN 'CRITICAL'
        WHEN qh.duration_ms > pb.threshold_duration_ms * 1.5 THEN 'HIGH'
        WHEN qh.duration_ms > pb.threshold_duration_ms THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    'PERFORMANCE_ANOMALY' as alert_type,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM system.query.history qh
JOIN query_performance_raw qpr ON qh.query_id = qpr.query_id
JOIN performance_baselines pb ON qpr.query_hash = pb.query_hash 
    AND qh.workspace_id = pb.workspace_id
    AND qh.user_id = pb.user_id
WHERE qh.start_time >= current_timestamp() - INTERVAL 4 HOURS
    AND qh.execution_status = 'FINISHED'
    AND (qh.duration_ms > pb.threshold_duration_ms OR qh.compute_cost_dbu > pb.threshold_cost_dbu)
ORDER BY duration_deviation_pct DESC, cost_deviation_pct DESC;

-- Resource utilization analysis
CREATE OR REPLACE VIEW v_resource_utilization
COMMENT 'Real-time resource utilization analysis for rightsizing recommendations'
AS
SELECT 
    cluster_id,
    workspace_id,
    DATE(event_time) as usage_date,
    HOUR(event_time) as usage_hour,
    COUNT(DISTINCT user_name) as active_users,
    COUNT(*) as total_queries,
    AVG(cluster_size) as avg_cluster_size,
    MAX(cluster_size) as max_cluster_size,
    MIN(cluster_size) as min_cluster_size,
    -- Utilization metrics
    SUM(CASE WHEN cluster_size > 0 THEN 1 ELSE 0 END) as active_minutes,
    COUNT(*) as total_minutes,
    ROUND(SUM(CASE WHEN cluster_size > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as utilization_pct,
    -- Rightsizing recommendations
    CASE 
        WHEN ROUND(SUM(CASE WHEN cluster_size > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) < 30 THEN 'UNDER_UTILIZED'
        WHEN ROUND(SUM(CASE WHEN cluster_size > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) > 80 THEN 'OVER_UTILIZED'
        ELSE 'OPTIMALLY_UTILIZED'
    END as utilization_category,
    CASE 
        WHEN ROUND(SUM(CASE WHEN cluster_size > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) < 30 THEN 'Consider downsizing cluster or using serverless'
        WHEN ROUND(SUM(CASE WHEN cluster_size > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) > 80 THEN 'Consider increasing cluster size or adding auto-scaling'
        ELSE 'Current configuration appears optimal'
    END as recommendation
FROM system.compute.cluster_events
WHERE event_time >= current_date() - INTERVAL 7 DAYS
    AND event_type IN ('RUNNING', 'RESIZING', 'TERMINATING')
GROUP BY cluster_id, workspace_id, DATE(event_time), HOUR(event_time)
ORDER BY usage_date DESC, usage_hour DESC;

-- Query complexity analysis
CREATE OR REPLACE VIEW v_query_complexity
COMMENT 'Analyze query complexity for optimization guidance'
AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    execution_status,
    query_text,
    -- Complexity indicators
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'SELECT', ''))) / 6 as select_count,
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'JOIN', ''))) / 4 as join_count,
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WHERE', ''))) / 5 as where_count,
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'GROUP BY', ''))) / 8 as group_by_count,
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'ORDER BY', ''))) / 8 as order_by_count,
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'UNION', ''))) / 5 as union_count,
    (LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WINDOW', ''))) / 6 as window_count,
    -- Overall complexity score (1-10)
    LEAST(10, GREATEST(1, 
        1 + 
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'SELECT', ''))) / 6) * 0.5 +
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'JOIN', ''))) / 4) * 1.0 +
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WHERE', ''))) / 5) * 0.3 +
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'GROUP BY', ''))) / 8) * 0.8 +
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'ORDER BY', ''))) / 8) * 0.6 +
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'UNION', ''))) / 5) * 0.7 +
        ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WINDOW', ''))) / 6) * 0.9 +
        (LENGTH(query_text) / 1000) * 0.1
    )) as complexity_score,
    -- Complexity category
    CASE 
        WHEN LEAST(10, GREATEST(1, 
            1 + 
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'SELECT', ''))) / 6) * 0.5 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'JOIN', ''))) / 4) * 1.0 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WHERE', ''))) / 5) * 0.3 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'GROUP BY', ''))) / 8) * 0.8 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'ORDER BY', ''))) / 8) * 0.6 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'UNION', ''))) / 5) * 0.7 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WINDOW', ''))) / 6) * 0.9 +
            (LENGTH(query_text) / 1000) * 0.1
        )) > 7 THEN 'HIGH'
        WHEN LEAST(10, GREATEST(1, 
            1 + 
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'SELECT', ''))) / 6) * 0.5 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'JOIN', ''))) / 4) * 1.0 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WHERE', ''))) / 5) * 0.3 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'GROUP BY', ''))) / 8) * 0.8 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'ORDER BY', ''))) / 8) * 0.6 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'UNION', ''))) / 5) * 0.7 +
            ((LENGTH(query_text) - LENGTH(REPLACE(UPPER(query_text), 'WINDOW', ''))) / 6) * 0.9 +
            (LENGTH(query_text) / 1000) * 0.1
        )) > 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END as complexity_category
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
    AND LENGTH(query_text) > 100  -- Filter out very simple queries
ORDER BY complexity_score DESC, duration_ms DESC;