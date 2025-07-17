-- Health Check Procedures for Query Optimization System
-- Quick health checks that can be run anytime to verify system status

-- ============================================================
-- QUICK HEALTH CHECK - Run this for immediate status
-- ============================================================

-- System Health Overview
SELECT 
    'SYSTEM_HEALTH_OVERVIEW' as check_type,
    current_timestamp() as check_time,
    (
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_catalog = 'mcp' 
            AND table_schema = 'query_optimization'
    ) as total_tables,
    (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.query_performance_raw 
        WHERE start_time >= current_date() - INTERVAL 1 DAY
    ) as queries_last_24h,
    (
        SELECT MAX(start_time) 
        FROM mcp.query_optimization.query_performance_raw
    ) as latest_query_time,
    (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.v_current_slow_queries
        WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ) as active_slow_query_alerts,
    (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.v_current_expensive_queries
        WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ) as active_cost_alerts,
    (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.v_current_failed_queries
        WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ) as active_failure_alerts,
    CASE 
        WHEN (SELECT MAX(start_time) FROM mcp.query_optimization.query_performance_raw) >= current_timestamp() - INTERVAL 2 HOURS 
        THEN 'HEALTHY'
        WHEN (SELECT MAX(start_time) FROM mcp.query_optimization.query_performance_raw) >= current_timestamp() - INTERVAL 24 HOURS 
        THEN 'WARNING'
        ELSE 'CRITICAL'
    END as overall_health_status;

-- ============================================================
-- DETAILED COMPONENT HEALTH CHECKS
-- ============================================================

-- Core Tables Health
SELECT 
    'CORE_TABLES_HEALTH' as check_type,
    table_name,
    row_count,
    latest_date,
    hours_since_latest,
    partition_count,
    CASE 
        WHEN row_count = 0 THEN 'EMPTY'
        WHEN hours_since_latest <= 2 THEN 'HEALTHY'
        WHEN hours_since_latest <= 24 THEN 'WARNING'
        ELSE 'STALE'
    END as health_status,
    current_timestamp() as check_time
FROM (
    SELECT 
        'query_performance_raw' as table_name,
        COUNT(*) as row_count,
        MAX(start_time) as latest_date,
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(start_time))) / 3600.0 as hours_since_latest,
        COUNT(DISTINCT DATE(start_time)) as partition_count
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 7 DAYS
    
    UNION ALL
    
    SELECT 
        'query_patterns' as table_name,
        COUNT(*) as row_count,
        MAX(last_seen) as latest_date,
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(last_seen))) / 3600.0 as hours_since_latest,
        COUNT(DISTINCT DATE(last_seen)) as partition_count
    FROM mcp.query_optimization.query_patterns
    WHERE last_seen >= current_date() - INTERVAL 7 DAYS
    
    UNION ALL
    
    SELECT 
        'optimization_tracking' as table_name,
        COUNT(*) as row_count,
        MAX(updated_at) as latest_date,
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(updated_at))) / 3600.0 as hours_since_latest,
        COUNT(DISTINCT implementation_date) as partition_count
    FROM mcp.query_optimization.optimization_tracking
    WHERE updated_at >= current_date() - INTERVAL 7 DAYS
    
    UNION ALL
    
    SELECT 
        'performance_baselines' as table_name,
        COUNT(*) as row_count,
        MAX(updated_at) as latest_date,
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(updated_at))) / 3600.0 as hours_since_latest,
        COUNT(DISTINCT baseline_period_start) as partition_count
    FROM mcp.query_optimization.performance_baselines
    WHERE updated_at >= current_date() - INTERVAL 7 DAYS
);

-- Materialized Views Health
SELECT 
    'MATERIALIZED_VIEWS_HEALTH' as check_type,
    view_name,
    record_count,
    latest_date,
    unique_workspaces,
    unique_users,
    CASE 
        WHEN record_count = 0 THEN 'EMPTY'
        WHEN latest_date >= current_date() - INTERVAL 1 DAY THEN 'HEALTHY'
        WHEN latest_date >= current_date() - INTERVAL 3 DAYS THEN 'WARNING'
        ELSE 'STALE'
    END as health_status,
    current_timestamp() as check_time
FROM (
    SELECT 
        'mv_daily_performance' as view_name,
        COUNT(*) as record_count,
        MAX(query_date) as latest_date,
        COUNT(DISTINCT workspace_id) as unique_workspaces,
        COUNT(DISTINCT user_id) as unique_users
    FROM mcp.query_optimization.mv_daily_performance
    WHERE query_date >= current_date() - INTERVAL 7 DAYS
    
    UNION ALL
    
    SELECT 
        'mv_hourly_performance' as view_name,
        COUNT(*) as record_count,
        MAX(query_date) as latest_date,
        COUNT(DISTINCT workspace_id) as unique_workspaces,
        COUNT(DISTINCT user_id) as unique_users
    FROM mcp.query_optimization.mv_hourly_performance
    WHERE query_date >= current_date() - INTERVAL 7 DAYS
    
    UNION ALL
    
    SELECT 
        'mv_user_performance' as view_name,
        COUNT(*) as record_count,
        MAX(last_query) as latest_date,
        COUNT(DISTINCT workspace_id) as unique_workspaces,
        COUNT(DISTINCT user_id) as unique_users
    FROM mcp.query_optimization.mv_user_performance
    
    UNION ALL
    
    SELECT 
        'mv_pattern_performance' as view_name,
        COUNT(*) as record_count,
        MAX(last_seen) as latest_date,
        COUNT(DISTINCT workspace_id) as unique_workspaces,
        COUNT(DISTINCT user_id) as unique_users
    FROM mcp.query_optimization.mv_pattern_performance
);

-- Alert System Health
SELECT 
    'ALERT_SYSTEM_HEALTH' as check_type,
    alert_category,
    current_active_alerts,
    alerts_last_hour,
    alerts_last_24h,
    max_severity,
    CASE 
        WHEN alerts_last_hour > 0 THEN 'ACTIVE'
        WHEN alerts_last_24h > 0 THEN 'RECENT_ACTIVITY'
        ELSE 'QUIET'
    END as alert_status,
    current_timestamp() as check_time
FROM (
    SELECT 
        'slow_queries' as alert_category,
        COUNT(*) as current_active_alerts,
        COUNT(CASE WHEN alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR THEN 1 END) as alerts_last_hour,
        COUNT(CASE WHEN alert_timestamp >= current_timestamp() - INTERVAL 24 HOURS THEN 1 END) as alerts_last_24h,
        MAX(severity) as max_severity
    FROM mcp.query_optimization.v_current_slow_queries
    
    UNION ALL
    
    SELECT 
        'expensive_queries' as alert_category,
        COUNT(*) as current_active_alerts,
        COUNT(CASE WHEN alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR THEN 1 END) as alerts_last_hour,
        COUNT(CASE WHEN alert_timestamp >= current_timestamp() - INTERVAL 24 HOURS THEN 1 END) as alerts_last_24h,
        MAX(severity) as max_severity
    FROM mcp.query_optimization.v_current_expensive_queries
    
    UNION ALL
    
    SELECT 
        'failed_queries' as alert_category,
        COUNT(*) as current_active_alerts,
        COUNT(CASE WHEN alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR THEN 1 END) as alerts_last_hour,
        COUNT(CASE WHEN alert_timestamp >= current_timestamp() - INTERVAL 24 HOURS THEN 1 END) as alerts_last_24h,
        MAX(severity) as max_severity
    FROM mcp.query_optimization.v_current_failed_queries
);

-- System Table Access Health
SELECT 
    'SYSTEM_TABLE_ACCESS' as check_type,
    system_table,
    CASE 
        WHEN access_test = 'SUCCESS' THEN recent_record_count
        ELSE 0
    END as recent_records,
    access_test as access_status,
    CASE 
        WHEN access_test = 'SUCCESS' AND recent_record_count > 0 THEN 'HEALTHY'
        WHEN access_test = 'SUCCESS' AND recent_record_count = 0 THEN 'WARNING'
        ELSE 'FAILED'
    END as health_status,
    current_timestamp() as check_time
FROM (
    SELECT 
        'system.query.history' as system_table,
        CASE 
            WHEN COUNT(*) >= 0 THEN 'SUCCESS'
            ELSE 'FAILED'
        END as access_test,
        COUNT(*) as recent_record_count
    FROM system.query.history 
    WHERE start_time >= current_timestamp() - INTERVAL 2 HOURS
    
    UNION ALL
    
    SELECT 
        'system.billing.usage' as system_table,
        CASE 
            WHEN COUNT(*) >= 0 THEN 'SUCCESS'
            ELSE 'FAILED'
        END as access_test,
        COUNT(*) as recent_record_count
    FROM system.billing.usage 
    WHERE DATE(usage_start_time) >= current_date() - INTERVAL 1 DAY
    
    UNION ALL
    
    SELECT 
        'system.compute.clusters' as system_table,
        CASE 
            WHEN COUNT(*) >= 0 THEN 'SUCCESS'
            ELSE 'FAILED'
        END as access_test,
        COUNT(*) as recent_record_count
    FROM system.compute.clusters
);

-- ============================================================
-- PERFORMANCE HEALTH METRICS
-- ============================================================

-- Query Processing Performance
SELECT 
    'PROCESSING_PERFORMANCE' as check_type,
    metric_name,
    metric_value,
    unit,
    CASE 
        WHEN metric_name = 'avg_processing_time_ms' AND metric_value < 5000 THEN 'GOOD'
        WHEN metric_name = 'avg_processing_time_ms' AND metric_value < 15000 THEN 'ACCEPTABLE'
        WHEN metric_name = 'avg_processing_time_ms' THEN 'SLOW'
        WHEN metric_name = 'queries_per_minute' AND metric_value > 10 THEN 'HIGH'
        WHEN metric_name = 'queries_per_minute' AND metric_value > 1 THEN 'NORMAL'
        WHEN metric_name = 'queries_per_minute' THEN 'LOW'
        WHEN metric_name = 'failure_rate_pct' AND metric_value < 5 THEN 'GOOD'
        WHEN metric_name = 'failure_rate_pct' AND metric_value < 15 THEN 'ACCEPTABLE'
        WHEN metric_name = 'failure_rate_pct' THEN 'HIGH'
        ELSE 'UNKNOWN'
    END as performance_level,
    current_timestamp() as check_time
FROM (
    SELECT 
        'avg_processing_time_ms' as metric_name,
        AVG(duration_ms) as metric_value,
        'milliseconds' as unit
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    
    UNION ALL
    
    SELECT 
        'queries_per_minute' as metric_name,
        COUNT(*) / 60.0 as metric_value,
        'queries/minute' as unit
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    
    UNION ALL
    
    SELECT 
        'failure_rate_pct' as metric_name,
        COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as metric_value,
        'percentage' as unit
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
);

-- Data Quality Health
SELECT 
    'DATA_QUALITY_HEALTH' as check_type,
    quality_check,
    issue_count,
    total_count,
    issue_percentage,
    CASE 
        WHEN issue_percentage = 0 THEN 'EXCELLENT'
        WHEN issue_percentage < 1 THEN 'GOOD'
        WHEN issue_percentage < 5 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as quality_level,
    current_timestamp() as check_time
FROM (
    SELECT 
        'null_query_ids' as quality_check,
        COUNT(CASE WHEN query_id IS NULL THEN 1 END) as issue_count,
        COUNT(*) as total_count,
        COUNT(CASE WHEN query_id IS NULL THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as issue_percentage
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 1 DAY
    
    UNION ALL
    
    SELECT 
        'negative_durations' as quality_check,
        COUNT(CASE WHEN duration_ms < 0 THEN 1 END) as issue_count,
        COUNT(*) as total_count,
        COUNT(CASE WHEN duration_ms < 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as issue_percentage
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 1 DAY
    
    UNION ALL
    
    SELECT 
        'negative_costs' as quality_check,
        COUNT(CASE WHEN compute_cost_dbu < 0 THEN 1 END) as issue_count,
        COUNT(*) as total_count,
        COUNT(CASE WHEN compute_cost_dbu < 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as issue_percentage
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 1 DAY
    
    UNION ALL
    
    SELECT 
        'invalid_optimization_scores' as quality_check,
        COUNT(CASE WHEN optimization_score < 1 OR optimization_score > 10 THEN 1 END) as issue_count,
        COUNT(*) as total_count,
        COUNT(CASE WHEN optimization_score < 1 OR optimization_score > 10 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as issue_percentage
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 1 DAY
        AND optimization_score IS NOT NULL
);

-- ============================================================
-- CRITICAL ISSUE DETECTION
-- ============================================================

-- Critical Issues that Require Immediate Attention
SELECT 
    'CRITICAL_ISSUES' as check_type,
    issue_type,
    issue_description,
    severity,
    affected_count,
    CASE 
        WHEN issue_type = 'NO_RECENT_DATA' AND affected_count > 0 THEN 'CRITICAL'
        WHEN issue_type = 'HIGH_FAILURE_RATE' AND affected_count > 20 THEN 'CRITICAL'
        WHEN issue_type = 'SYSTEM_ACCESS_FAILED' AND affected_count > 0 THEN 'CRITICAL'
        WHEN issue_type = 'DATA_CORRUPTION' AND affected_count > 0 THEN 'CRITICAL'
        ELSE 'INFO'
    END as issue_severity,
    current_timestamp() as check_time
FROM (
    -- Check for no recent data
    SELECT 
        'NO_RECENT_DATA' as issue_type,
        'No query data received in the last 4 hours' as issue_description,
        'CRITICAL' as severity,
        CASE 
            WHEN MAX(start_time) < current_timestamp() - INTERVAL 4 HOURS OR MAX(start_time) IS NULL THEN 1
            ELSE 0
        END as affected_count
    FROM mcp.query_optimization.query_performance_raw
    
    UNION ALL
    
    -- Check for high failure rate
    SELECT 
        'HIGH_FAILURE_RATE' as issue_type,
        'High query failure rate detected in last hour' as issue_description,
        'HIGH' as severity,
        COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as affected_count
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    
    UNION ALL
    
    -- Check for system table access issues
    SELECT 
        'SYSTEM_ACCESS_FAILED' as issue_type,
        'Unable to access system tables' as issue_description,
        'CRITICAL' as severity,
        CASE 
            WHEN (SELECT COUNT(*) FROM system.query.history WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR) = 0 
            THEN 1 
            ELSE 0 
        END as affected_count
    
    UNION ALL
    
    -- Check for data corruption indicators
    SELECT 
        'DATA_CORRUPTION' as issue_type,
        'Potential data corruption detected' as issue_description,
        'CRITICAL' as severity,
        COUNT(*) as affected_count
    FROM mcp.query_optimization.query_performance_raw
    WHERE start_time >= current_date() - INTERVAL 1 DAY
        AND (duration_ms < 0 OR compute_cost_dbu < 0 OR query_id IS NULL)
)
WHERE affected_count > 0;

-- ============================================================
-- HEALTH CHECK SUMMARY
-- ============================================================

-- Overall Health Summary
SELECT 
    'HEALTH_SUMMARY' as check_type,
    'OVERALL_SYSTEM_STATUS' as component,
    current_timestamp() as check_time,
    CASE 
        -- Critical issues check
        WHEN (SELECT COUNT(*) FROM mcp.query_optimization.query_performance_raw WHERE start_time >= current_timestamp() - INTERVAL 4 HOURS) = 0 
        THEN 'CRITICAL'
        
        -- System table access check
        WHEN (SELECT COUNT(*) FROM system.query.history WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR) = 0
        THEN 'CRITICAL'
        
        -- High failure rate check
        WHEN (
            SELECT COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) 
            FROM mcp.query_optimization.query_performance_raw 
            WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
        ) > 50
        THEN 'WARNING'
        
        -- Data freshness check
        WHEN (SELECT MAX(start_time) FROM mcp.query_optimization.query_performance_raw) < current_timestamp() - INTERVAL 2 HOURS
        THEN 'WARNING'
        
        -- All good
        ELSE 'HEALTHY'
    END as health_status,
    CASE 
        WHEN (SELECT COUNT(*) FROM mcp.query_optimization.query_performance_raw WHERE start_time >= current_timestamp() - INTERVAL 4 HOURS) = 0 
        THEN 'No query data received in last 4 hours - check ETL pipeline'
        
        WHEN (SELECT COUNT(*) FROM system.query.history WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR) = 0
        THEN 'Cannot access system tables - check permissions'
        
        WHEN (
            SELECT COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) 
            FROM mcp.query_optimization.query_performance_raw 
            WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
        ) > 50
        THEN 'High failure rate detected - investigate query issues'
        
        WHEN (SELECT MAX(start_time) FROM mcp.query_optimization.query_performance_raw) < current_timestamp() - INTERVAL 2 HOURS
        THEN 'Data is stale - check ETL schedule'
        
        ELSE 'All systems operational'
    END as status_message,
    (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.query_performance_raw 
        WHERE start_time >= current_date() - INTERVAL 1 DAY
    ) as queries_processed_24h,
    (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.v_current_slow_queries
        WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ) + (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.v_current_expensive_queries
        WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ) + (
        SELECT COUNT(*) 
        FROM mcp.query_optimization.v_current_failed_queries
        WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ) as active_alerts;

-- Quick Performance Stats
SELECT 
    'PERFORMANCE_STATS' as check_type,
    'LAST_24_HOURS' as time_period,
    COUNT(*) as total_queries,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT workspace_id) as unique_workspaces,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries,
    ROUND(COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) * 100.0 / COUNT(*), 2) as failure_rate_pct,
    current_timestamp() as check_time
FROM mcp.query_optimization.query_performance_raw
WHERE start_time >= current_date() - INTERVAL 1 DAY;