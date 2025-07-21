-- Databricks Query Optimization - Performance Tables (converted from Materialized Views)
-- This script creates Delta tables for pre-computed aggregations and dashboards
-- These tables are optimized for real-time analysis and Genie Space integration
-- NOTE: Converted from materialized views for POC testing (no DLT pipeline required)

USE mcp.query_optimization;

-- Hourly performance metrics for trend analysis
DROP TABLE IF EXISTS hourly_performance;
CREATE TABLE hourly_performance
(
    query_date DATE,
    query_hour INT,
    workspace_id BIGINT,
    user_id BIGINT,
    query_count BIGINT,
    unique_query_patterns BIGINT,
    avg_duration_ms DOUBLE,
    median_duration_ms DOUBLE,
    p95_duration_ms DOUBLE,
    p99_duration_ms DOUBLE,
    total_cost_dbu DOUBLE,
    avg_cost_dbu DOUBLE,
    total_bytes_read BIGINT,
    avg_bytes_read DOUBLE,
    total_rows_read BIGINT,
    avg_rows_read DOUBLE,
    successful_queries BIGINT,
    failed_queries BIGINT,
    success_rate DECIMAL(10,4),
    avg_complexity_score DOUBLE,
    avg_optimization_score DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (query_date)
COMMENT 'Hourly query performance metrics for trend analysis';

-- Daily performance summary for executive reporting
DROP TABLE IF EXISTS daily_performance;
CREATE TABLE daily_performance
(
    query_date DATE,
    workspace_id BIGINT,
    user_id BIGINT,
    total_queries BIGINT,
    unique_patterns BIGINT,
    avg_duration_ms DOUBLE,
    p95_duration_ms DOUBLE,
    total_cost_dbu DOUBLE,
    avg_cost_dbu DOUBLE,
    total_bytes_read BIGINT,
    successful_queries BIGINT,
    failed_queries BIGINT,
    success_rate DECIMAL(10,4),
    fast_queries BIGINT,
    medium_queries BIGINT,
    slow_queries BIGINT,
    low_cost_queries BIGINT,
    medium_cost_queries BIGINT,
    high_cost_queries BIGINT,
    cost_per_second DOUBLE,
    bytes_per_second DOUBLE,
    avg_optimization_score DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (query_date)
COMMENT 'Daily query performance summary for executive reporting';

-- Query pattern performance for optimization prioritization
DROP TABLE IF EXISTS pattern_performance;
CREATE TABLE pattern_performance
(
    query_hash STRING,
    pattern_type STRING,
    pattern_description STRING,
    workspace_id BIGINT,
    execution_count BIGINT,
    unique_users BIGINT,
    avg_duration_ms DOUBLE,
    p95_duration_ms DOUBLE,
    total_cost_dbu DOUBLE,
    avg_cost_dbu DOUBLE,
    total_bytes_read BIGINT,
    avg_bytes_read DOUBLE,
    successful_executions BIGINT,
    failed_executions BIGINT,
    success_rate DECIMAL(10,4),
    avg_complexity_score DOUBLE,
    avg_optimization_score DOUBLE,
    optimization_priority STRING,
    optimization_recommendations STRING,
    optimization_impact STRING,
    estimated_monthly_savings_dbu DOUBLE,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (pattern_type)
COMMENT 'Query pattern performance metrics for optimization prioritization';

-- User performance summary for individual analysis
DROP TABLE IF EXISTS user_performance;
CREATE TABLE user_performance
(
    user_id BIGINT,
    user_email STRING,
    workspace_id BIGINT,
    total_queries BIGINT,
    unique_patterns BIGINT,
    active_days BIGINT,
    avg_duration_ms DOUBLE,
    p95_duration_ms DOUBLE,
    total_cost_dbu DOUBLE,
    avg_cost_dbu DOUBLE,
    total_bytes_read BIGINT,
    successful_queries BIGINT,
    failed_queries BIGINT,
    success_rate DECIMAL(10,4),
    fast_queries BIGINT,
    medium_queries BIGINT,
    slow_queries BIGINT,
    avg_optimization_score DOUBLE,
    cost_per_query DOUBLE,
    avg_duration_per_query DOUBLE,
    optimization_opportunity_score INT,
    first_query TIMESTAMP,
    last_query TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (workspace_id)
COMMENT 'User performance summary for individual analysis and coaching';

-- Real-time performance alerts summary
DROP TABLE IF EXISTS performance_alerts;
CREATE TABLE performance_alerts
(
    alert_date DATE,
    alert_hour INT,
    workspace_id BIGINT,
    user_id BIGINT,
    long_running_queries BIGINT,
    high_cost_queries BIGINT,
    failed_queries BIGINT,
    large_scan_queries BIGINT,
    total_alerts BIGINT,
    avg_severity_score DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (alert_date)
COMMENT 'Performance alerts summary for real-time monitoring';

-- Query performance categorized (for Genie Space 1)
DROP TABLE IF EXISTS query_performance_categorized;
CREATE TABLE query_performance_categorized
(
    statement_id STRING,
    statement_text STRING,
    executed_by STRING,
    executed_as STRING,
    total_duration_ms BIGINT,
    execution_duration_ms BIGINT,
    result_fetch_duration_ms BIGINT,
    read_bytes BIGINT,
    read_rows BIGINT,
    read_partitions BIGINT,
    error_message STRING,
    warehouse_id STRING,
    compute_type STRING,
    end_time TIMESTAMP,
    performance_category STRING,
    bytes_per_row_efficiency DOUBLE,
    optimization_flag STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (DATE(end_time))
COMMENT 'Query performance with categorization using business logic (SLOW/MODERATE/FAST)';

-- Current slow queries (for Genie Space 1)
DROP TABLE IF EXISTS current_slow_queries;
CREATE TABLE current_slow_queries
(
    statement_id STRING,
    executed_by STRING,
    warehouse_id STRING,
    execution_duration_ms BIGINT,
    read_bytes BIGINT,
    performance_impact_score INT,
    suggested_optimization STRING,
    first_seen TIMESTAMP,
    occurrence_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (DATE(first_seen))
COMMENT 'Real-time view of performance issues requiring attention';

-- Resource utilization alerts (for Genie Space 1)
DROP TABLE IF EXISTS resource_utilization_alerts;
CREATE TABLE resource_utilization_alerts
(
    warehouse_id STRING,
    time_window TIMESTAMP,
    avg_cpu_utilization DOUBLE,
    avg_memory_utilization DOUBLE,
    query_count BIGINT,
    total_data_processed_gb DOUBLE,
    cache_hit_ratio DOUBLE,
    alert_level STRING,
    bottleneck_type STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (DATE(time_window))
COMMENT 'Resource usage alerts and efficiency metrics';

COMMENT ON DATABASE mcp.query_optimization IS 'Query Optimization database with Delta tables (converted from materialized views for POC)';