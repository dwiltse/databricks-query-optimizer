-- Real-time Alerts and Monitoring
-- Queries for immediate detection of performance issues and anomalies

-- Query 1: Current Performance Alerts
SELECT 
    'CURRENT_ALERTS' as alert_category,
    COUNT(*) as total_active_alerts,
    COUNT(CASE WHEN alert_type = 'SLOW_QUERY' THEN 1 END) as slow_query_alerts,
    COUNT(CASE WHEN alert_type = 'EXPENSIVE_QUERY' THEN 1 END) as expensive_query_alerts,
    COUNT(CASE WHEN alert_type = 'QUERY_FAILURE' THEN 1 END) as failure_alerts,
    COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_alerts,
    COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_alerts,
    COUNT(CASE WHEN severity = 'MEDIUM' THEN 1 END) as medium_alerts,
    MAX(alert_timestamp) as latest_alert_time,
    MIN(alert_timestamp) as earliest_alert_time
FROM (
    SELECT *, 'SLOW_QUERY' as alert_type FROM mcp.query_optimization.v_current_slow_queries
    UNION ALL
    SELECT *, 'EXPENSIVE_QUERY' as alert_type FROM mcp.query_optimization.v_current_expensive_queries
    UNION ALL
    SELECT *, 'QUERY_FAILURE' as alert_type FROM mcp.query_optimization.v_current_failed_queries
) all_alerts;

-- Query 2: Slow Query Alerts (Last Hour)
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    ROUND(duration_ms / 1000.0, 2) as duration_seconds,
    ROUND(duration_ms / 60000.0, 2) as duration_minutes,
    compute_cost_dbu,
    bytes_read,
    ROUND(bytes_read / (1024 * 1024 * 1024.0), 2) as gb_read,
    execution_status,
    query_preview,
    severity,
    alert_timestamp,
    -- Context for alert
    CASE 
        WHEN duration_ms > 1800000 THEN 'Query running for over 30 minutes - investigate immediately'
        WHEN duration_ms > 900000 THEN 'Query running for over 15 minutes - monitor closely'
        WHEN duration_ms > 300000 THEN 'Query running for over 5 minutes - review efficiency'
        ELSE 'Performance monitoring alert'
    END as alert_message,
    -- Suggested immediate actions
    CASE 
        WHEN duration_ms > 1800000 THEN 'Consider canceling query and reviewing execution plan'
        WHEN duration_ms > 900000 THEN 'Check cluster resources and query complexity'
        WHEN duration_ms > 300000 THEN 'Review for optimization opportunities'
        ELSE 'Monitor for completion'
    END as suggested_action,
    -- Performance relative to normal
    ROUND(duration_ms / (SELECT AVG(duration_ms) FROM system.query.history 
                        WHERE start_time >= current_date() - INTERVAL 7 DAYS
                        AND execution_status = 'FINISHED'), 2) as duration_vs_avg
FROM mcp.query_optimization.v_current_slow_queries
WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY duration_ms DESC, alert_timestamp DESC;

-- Query 3: Expensive Query Alerts (Last Hour)
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    bytes_read,
    ROUND(bytes_read / (1024 * 1024 * 1024.0), 2) as gb_read,
    execution_status,
    query_preview,
    severity,
    alert_timestamp,
    -- Cost analysis
    ROUND(compute_cost_dbu / (duration_ms / 1000.0), 4) as cost_per_second,
    ROUND(compute_cost_dbu / (bytes_read / (1024 * 1024 * 1024.0)), 4) as cost_per_gb,
    -- Alert context
    CASE 
        WHEN compute_cost_dbu > 100 THEN 'Critical: Query consuming excessive resources'
        WHEN compute_cost_dbu > 50 THEN 'High: Query cost significantly above normal'
        WHEN compute_cost_dbu > 20 THEN 'Medium: Query cost above recommended threshold'
        ELSE 'Low: Query cost monitoring alert'
    END as alert_message,
    -- Immediate recommendations
    CASE 
        WHEN compute_cost_dbu > 100 THEN 'Review query immediately - consider cancellation'
        WHEN compute_cost_dbu > 50 THEN 'Optimize data access patterns and filters'
        WHEN compute_cost_dbu > 20 THEN 'Review for SELECT * usage and add appropriate filters'
        ELSE 'Monitor query completion'
    END as immediate_recommendation,
    -- Cost relative to normal
    ROUND(compute_cost_dbu / (SELECT AVG(compute_cost_dbu) FROM system.query.history 
                             WHERE start_time >= current_date() - INTERVAL 7 DAYS
                             AND execution_status = 'FINISHED'), 2) as cost_vs_avg
FROM mcp.query_optimization.v_current_expensive_queries
WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY compute_cost_dbu DESC, alert_timestamp DESC;

-- Query 4: Query Failure Alerts (Last Hour)
SELECT 
    query_id,
    workspace_id,
    user_id,
    user_email,
    start_time,
    duration_ms,
    compute_cost_dbu,
    execution_status,
    error_message,
    query_preview,
    failure_category,
    severity,
    alert_timestamp,
    -- Failure analysis
    CASE 
        WHEN failure_category = 'TIMEOUT' THEN 'Query timed out - may need performance optimization'
        WHEN failure_category = 'MEMORY' THEN 'Out of memory - consider query optimization or cluster sizing'
        WHEN failure_category = 'PERMISSION' THEN 'Access denied - check user permissions'
        WHEN failure_category = 'SYNTAX' THEN 'SQL syntax error - review query structure'
        WHEN failure_category = 'CONNECTION' THEN 'Connection issue - check network and cluster status'
        WHEN failure_category = 'RESOURCE' THEN 'Resource constraint - check cluster capacity'
        ELSE 'Unknown failure - investigate error details'
    END as failure_analysis,
    -- Immediate actions
    CASE 
        WHEN failure_category = 'TIMEOUT' THEN 'Optimize query or increase timeout settings'
        WHEN failure_category = 'MEMORY' THEN 'Optimize query or increase cluster memory'
        WHEN failure_category = 'PERMISSION' THEN 'Grant necessary permissions'
        WHEN failure_category = 'SYNTAX' THEN 'Fix SQL syntax errors'
        WHEN failure_category = 'CONNECTION' THEN 'Check cluster status and restart if needed'
        WHEN failure_category = 'RESOURCE' THEN 'Scale cluster or optimize resource usage'
        ELSE 'Investigate error message details'
    END as immediate_action,
    -- Check for pattern failures
    (SELECT COUNT(*) FROM mcp.query_optimization.v_current_failed_queries fq2 
     WHERE fq2.user_id = v_current_failed_queries.user_id 
     AND fq2.failure_category = v_current_failed_queries.failure_category
     AND fq2.alert_timestamp >= current_timestamp() - INTERVAL 4 HOURS) as similar_failures_4h
FROM mcp.query_optimization.v_current_failed_queries
WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY alert_timestamp DESC;

-- Query 5: Performance Anomaly Detection
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
    query_preview,
    baseline_avg_duration_ms,
    baseline_avg_cost_dbu,
    duration_deviation_ms,
    cost_deviation_dbu,
    duration_deviation_pct,
    cost_deviation_pct,
    severity,
    alert_timestamp,
    -- Anomaly analysis
    CASE 
        WHEN duration_deviation_pct > 200 THEN 'Critical performance degradation detected'
        WHEN duration_deviation_pct > 100 THEN 'Significant performance degradation'
        WHEN duration_deviation_pct > 50 THEN 'Moderate performance degradation'
        ELSE 'Minor performance variation'
    END as anomaly_analysis,
    -- Potential causes
    CASE 
        WHEN duration_deviation_pct > 200 AND cost_deviation_pct > 100 THEN 'Data volume increase or cluster performance issue'
        WHEN duration_deviation_pct > 100 AND cost_deviation_pct < 50 THEN 'Query execution plan changed'
        WHEN duration_deviation_pct > 50 AND bytes_read > baseline_avg_duration_ms * 1000 THEN 'Data scan increase'
        ELSE 'Normal variation or minor optimization opportunity'
    END as potential_cause,
    -- Recommended investigation
    CASE 
        WHEN duration_deviation_pct > 200 THEN 'Investigate immediately - check cluster health and data changes'
        WHEN duration_deviation_pct > 100 THEN 'Review query execution plan and recent data changes'
        WHEN duration_deviation_pct > 50 THEN 'Monitor trend and consider optimization'
        ELSE 'Continue monitoring'
    END as recommended_investigation
FROM mcp.query_optimization.v_performance_anomalies
WHERE alert_timestamp >= current_timestamp() - INTERVAL 4 HOURS
    AND severity IN ('CRITICAL', 'HIGH')
ORDER BY duration_deviation_pct DESC, alert_timestamp DESC;

-- Query 6: Resource Utilization Alerts
SELECT 
    cluster_id,
    workspace_id,
    usage_date,
    usage_hour,
    active_users,
    total_queries,
    avg_cluster_size,
    max_cluster_size,
    utilization_pct,
    utilization_category,
    recommendation,
    -- Alert conditions
    CASE 
        WHEN utilization_pct < 10 THEN 'CRITICAL_UNDERUTILIZED'
        WHEN utilization_pct < 30 THEN 'UNDERUTILIZED'
        WHEN utilization_pct > 95 THEN 'CRITICAL_OVERUTILIZED'
        WHEN utilization_pct > 80 THEN 'OVERUTILIZED'
        ELSE 'NORMAL'
    END as utilization_alert_level,
    -- Cost impact
    CASE 
        WHEN utilization_pct < 10 THEN 'High cost waste - consider shutting down cluster'
        WHEN utilization_pct < 30 THEN 'Moderate cost waste - consider downsizing'
        WHEN utilization_pct > 95 THEN 'Performance bottleneck - queries may be queued'
        WHEN utilization_pct > 80 THEN 'High utilization - consider scaling up'
        ELSE 'Optimal utilization'
    END as cost_impact,
    -- Immediate actions
    CASE 
        WHEN utilization_pct < 10 THEN 'Shutdown cluster or consolidate workloads'
        WHEN utilization_pct < 30 THEN 'Downsize cluster or schedule auto-termination'
        WHEN utilization_pct > 95 THEN 'Add nodes or enable auto-scaling'
        WHEN utilization_pct > 80 THEN 'Monitor closely and prepare to scale'
        ELSE 'No immediate action required'
    END as immediate_action,
    -- Estimated cost impact
    CASE 
        WHEN utilization_pct < 10 THEN avg_cluster_size * 0.9  -- 90% waste
        WHEN utilization_pct < 30 THEN avg_cluster_size * 0.6  -- 60% waste
        ELSE 0
    END as estimated_waste_factor
FROM mcp.query_optimization.v_resource_utilization
WHERE usage_date >= current_date() - INTERVAL 1 DAY
    AND (utilization_pct < 30 OR utilization_pct > 80)
ORDER BY 
    CASE 
        WHEN utilization_pct < 10 THEN 1
        WHEN utilization_pct > 95 THEN 2
        WHEN utilization_pct < 30 THEN 3
        WHEN utilization_pct > 80 THEN 4
        ELSE 5
    END,
    estimated_waste_factor DESC;

-- Query 7: Alert Summary Dashboard
SELECT 
    alert_category,
    alert_count,
    critical_count,
    high_count,
    medium_count,
    low_count,
    latest_alert,
    -- Alert trends
    CASE 
        WHEN alert_count > prev_hour_count * 1.5 THEN 'INCREASING'
        WHEN alert_count < prev_hour_count * 0.7 THEN 'DECREASING'
        ELSE 'STABLE'
    END as alert_trend,
    -- Priority assessment
    CASE 
        WHEN critical_count > 0 THEN 'CRITICAL_ATTENTION_REQUIRED'
        WHEN high_count > 5 THEN 'HIGH_ATTENTION_REQUIRED'
        WHEN medium_count > 10 THEN 'MEDIUM_ATTENTION_REQUIRED'
        ELSE 'NORMAL_MONITORING'
    END as priority_level
FROM (
    SELECT 
        'SLOW_QUERIES' as alert_category,
        COUNT(*) as alert_count,
        COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_count,
        COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_count,
        COUNT(CASE WHEN severity = 'MEDIUM' THEN 1 END) as medium_count,
        COUNT(CASE WHEN severity = 'LOW' THEN 1 END) as low_count,
        MAX(alert_timestamp) as latest_alert,
        (SELECT COUNT(*) FROM mcp.query_optimization.v_current_slow_queries 
         WHERE alert_timestamp >= current_timestamp() - INTERVAL 2 HOURS
         AND alert_timestamp < current_timestamp() - INTERVAL 1 HOUR) as prev_hour_count
    FROM mcp.query_optimization.v_current_slow_queries
    WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    
    UNION ALL
    
    SELECT 
        'EXPENSIVE_QUERIES' as alert_category,
        COUNT(*) as alert_count,
        COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_count,
        COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_count,
        COUNT(CASE WHEN severity = 'MEDIUM' THEN 1 END) as medium_count,
        COUNT(CASE WHEN severity = 'LOW' THEN 1 END) as low_count,
        MAX(alert_timestamp) as latest_alert,
        (SELECT COUNT(*) FROM mcp.query_optimization.v_current_expensive_queries 
         WHERE alert_timestamp >= current_timestamp() - INTERVAL 2 HOURS
         AND alert_timestamp < current_timestamp() - INTERVAL 1 HOUR) as prev_hour_count
    FROM mcp.query_optimization.v_current_expensive_queries
    WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    
    UNION ALL
    
    SELECT 
        'QUERY_FAILURES' as alert_category,
        COUNT(*) as alert_count,
        COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_count,
        COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_count,
        COUNT(CASE WHEN severity = 'MEDIUM' THEN 1 END) as medium_count,
        COUNT(CASE WHEN severity = 'LOW' THEN 1 END) as low_count,
        MAX(alert_timestamp) as latest_alert,
        (SELECT COUNT(*) FROM mcp.query_optimization.v_current_failed_queries 
         WHERE alert_timestamp >= current_timestamp() - INTERVAL 2 HOURS
         AND alert_timestamp < current_timestamp() - INTERVAL 1 HOUR) as prev_hour_count
    FROM mcp.query_optimization.v_current_failed_queries
    WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    
    UNION ALL
    
    SELECT 
        'PERFORMANCE_ANOMALIES' as alert_category,
        COUNT(*) as alert_count,
        COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_count,
        COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_count,
        COUNT(CASE WHEN severity = 'MEDIUM' THEN 1 END) as medium_count,
        COUNT(CASE WHEN severity = 'LOW' THEN 1 END) as low_count,
        MAX(alert_timestamp) as latest_alert,
        (SELECT COUNT(*) FROM mcp.query_optimization.v_performance_anomalies 
         WHERE alert_timestamp >= current_timestamp() - INTERVAL 2 HOURS
         AND alert_timestamp < current_timestamp() - INTERVAL 1 HOUR) as prev_hour_count
    FROM mcp.query_optimization.v_performance_anomalies
    WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
        AND severity IN ('CRITICAL', 'HIGH')
) alert_summary
ORDER BY 
    CASE 
        WHEN priority_level = 'CRITICAL_ATTENTION_REQUIRED' THEN 1
        WHEN priority_level = 'HIGH_ATTENTION_REQUIRED' THEN 2
        WHEN priority_level = 'MEDIUM_ATTENTION_REQUIRED' THEN 3
        ELSE 4
    END,
    critical_count DESC,
    high_count DESC;

-- Query 8: Alert Escalation Rules
SELECT 
    alert_rule,
    condition_description,
    escalation_level,
    notification_target,
    auto_action,
    last_triggered,
    trigger_count_24h
FROM (
    SELECT 
        'CRITICAL_SLOW_QUERY' as alert_rule,
        'Query running for more than 30 minutes' as condition_description,
        'IMMEDIATE' as escalation_level,
        'On-call Engineer + Team Lead' as notification_target,
        'Consider automatic cancellation' as auto_action,
        MAX(alert_timestamp) as last_triggered,
        COUNT(*) as trigger_count_24h
    FROM mcp.query_optimization.v_current_slow_queries
    WHERE severity = 'CRITICAL'
        AND alert_timestamp >= current_timestamp() - INTERVAL 24 HOURS
    
    UNION ALL
    
    SELECT 
        'HIGH_COST_QUERY' as alert_rule,
        'Query consuming more than 100 DBUs' as condition_description,
        'HIGH' as escalation_level,
        'FinOps Team + Query Owner' as notification_target,
        'Alert and recommend optimization' as auto_action,
        MAX(alert_timestamp) as last_triggered,
        COUNT(*) as trigger_count_24h
    FROM mcp.query_optimization.v_current_expensive_queries
    WHERE severity = 'CRITICAL'
        AND alert_timestamp >= current_timestamp() - INTERVAL 24 HOURS
    
    UNION ALL
    
    SELECT 
        'REPEATED_FAILURES' as alert_rule,
        'Same user having multiple query failures' as condition_description,
        'MEDIUM' as escalation_level,
        'User + Team Lead' as notification_target,
        'Provide optimization guidance' as auto_action,
        MAX(alert_timestamp) as last_triggered,
        COUNT(DISTINCT user_id) as trigger_count_24h
    FROM mcp.query_optimization.v_current_failed_queries
    WHERE alert_timestamp >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY user_id
    HAVING COUNT(*) >= 3
    
    UNION ALL
    
    SELECT 
        'RESOURCE_WASTE' as alert_rule,
        'Cluster utilization below 10%' as condition_description,
        'MEDIUM' as escalation_level,
        'Infrastructure Team' as notification_target,
        'Auto-terminate after 30 minutes' as auto_action,
        MAX(TIMESTAMP(CONCAT(usage_date, ' ', usage_hour, ':00:00'))) as last_triggered,
        COUNT(*) as trigger_count_24h
    FROM mcp.query_optimization.v_resource_utilization
    WHERE utilization_pct < 10
        AND usage_date >= current_date() - INTERVAL 1 DAY
) escalation_rules
WHERE trigger_count_24h > 0
ORDER BY 
    CASE 
        WHEN escalation_level = 'IMMEDIATE' THEN 1
        WHEN escalation_level = 'HIGH' THEN 2
        WHEN escalation_level = 'MEDIUM' THEN 3
        ELSE 4
    END,
    trigger_count_24h DESC;