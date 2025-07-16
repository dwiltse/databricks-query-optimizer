-- Core Delta Tables for Query Performance Data
-- These tables store historical query performance data for analysis

USE mcp.query_optimization;

-- Raw query performance data from system tables
CREATE TABLE IF NOT EXISTS query_performance_raw (
    query_id STRING COMMENT 'Unique identifier for the query',
    workspace_id STRING COMMENT 'Workspace where query was executed',
    user_id STRING COMMENT 'User who executed the query',
    user_email STRING COMMENT 'User email address',
    query_text STRING COMMENT 'Full SQL query text',
    query_hash STRING COMMENT 'Hash of query text for pattern matching',
    start_time TIMESTAMP COMMENT 'Query execution start time',
    end_time TIMESTAMP COMMENT 'Query execution end time',
    duration_ms BIGINT COMMENT 'Query execution duration in milliseconds',
    rows_read BIGINT COMMENT 'Number of rows read by the query',
    bytes_read BIGINT COMMENT 'Number of bytes read by the query',
    rows_produced BIGINT COMMENT 'Number of rows produced by the query',
    compute_cost_dbu DECIMAL(10,4) COMMENT 'DBU cost for query execution',
    execution_status STRING COMMENT 'Query execution status (FINISHED, FAILED, etc.)',
    error_message STRING COMMENT 'Error message if query failed',
    cluster_id STRING COMMENT 'Cluster ID where query was executed',
    warehouse_id STRING COMMENT 'SQL warehouse ID where query was executed',
    query_type STRING COMMENT 'Type of query (SELECT, INSERT, CREATE, etc.)',
    complexity_score DECIMAL(5,2) COMMENT 'Query complexity score (1-10)',
    optimization_score DECIMAL(5,2) COMMENT 'Query optimization score (1-10)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
) 
USING DELTA
PARTITIONED BY (DATE(start_time), workspace_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '10'
)
COMMENT 'Raw query performance data extracted from system tables';

-- Query pattern analysis for optimization opportunities
CREATE TABLE IF NOT EXISTS query_patterns (
    pattern_id STRING COMMENT 'Unique identifier for the query pattern',
    query_hash STRING COMMENT 'Hash representing the query pattern',
    pattern_type STRING COMMENT 'Type of pattern (SELECT_ALL, UNBOUNDED_SORT, etc.)',
    pattern_description STRING COMMENT 'Human-readable description of the pattern',
    query_template STRING COMMENT 'Templated version of the query',
    first_seen TIMESTAMP COMMENT 'First time this pattern was observed',
    last_seen TIMESTAMP COMMENT 'Last time this pattern was observed',
    occurrence_count BIGINT COMMENT 'Number of times this pattern occurred',
    avg_duration_ms BIGINT COMMENT 'Average execution time for this pattern',
    avg_cost_dbu DECIMAL(10,4) COMMENT 'Average DBU cost for this pattern',
    optimization_priority STRING COMMENT 'Priority level (HIGH, MEDIUM, LOW)',
    optimization_recommendations STRING COMMENT 'Specific optimization recommendations',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
PARTITIONED BY (DATE(last_seen))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Query pattern analysis for identifying optimization opportunities';

-- Optimization tracking for measuring impact
CREATE TABLE IF NOT EXISTS optimization_tracking (
    optimization_id STRING COMMENT 'Unique identifier for the optimization',
    query_id STRING COMMENT 'Original query identifier',
    query_hash STRING COMMENT 'Query pattern hash',
    workspace_id STRING COMMENT 'Workspace where optimization was applied',
    user_id STRING COMMENT 'User who implemented the optimization',
    optimization_type STRING COMMENT 'Type of optimization applied',
    optimization_description STRING COMMENT 'Description of the optimization',
    implementation_date DATE COMMENT 'Date when optimization was implemented',
    before_avg_duration_ms BIGINT COMMENT 'Average duration before optimization',
    after_avg_duration_ms BIGINT COMMENT 'Average duration after optimization',
    before_avg_cost_dbu DECIMAL(10,4) COMMENT 'Average cost before optimization',
    after_avg_cost_dbu DECIMAL(10,4) COMMENT 'Average cost after optimization',
    duration_improvement_pct DECIMAL(5,2) COMMENT 'Percentage improvement in duration',
    cost_improvement_pct DECIMAL(5,2) COMMENT 'Percentage improvement in cost',
    estimated_monthly_savings_usd DECIMAL(10,2) COMMENT 'Estimated monthly savings in USD',
    actual_monthly_savings_usd DECIMAL(10,2) COMMENT 'Actual monthly savings in USD',
    implementation_effort STRING COMMENT 'Implementation effort level (LOW, MEDIUM, HIGH)',
    status STRING COMMENT 'Optimization status (PLANNED, IMPLEMENTED, VERIFIED)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
PARTITIONED BY (implementation_date, workspace_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Tracks optimization implementations and their measured impact';

-- Query performance baselines for anomaly detection
CREATE TABLE IF NOT EXISTS performance_baselines (
    baseline_id STRING COMMENT 'Unique identifier for the baseline',
    query_hash STRING COMMENT 'Query pattern hash',
    workspace_id STRING COMMENT 'Workspace identifier',
    user_id STRING COMMENT 'User identifier',
    baseline_period_start DATE COMMENT 'Start date of baseline period',
    baseline_period_end DATE COMMENT 'End date of baseline period',
    baseline_avg_duration_ms BIGINT COMMENT 'Baseline average duration',
    baseline_p95_duration_ms BIGINT COMMENT 'Baseline 95th percentile duration',
    baseline_avg_cost_dbu DECIMAL(10,4) COMMENT 'Baseline average cost',
    baseline_success_rate DECIMAL(5,4) COMMENT 'Baseline success rate',
    baseline_execution_count BIGINT COMMENT 'Number of executions in baseline',
    threshold_duration_ms BIGINT COMMENT 'Alert threshold for duration',
    threshold_cost_dbu DECIMAL(10,4) COMMENT 'Alert threshold for cost',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
PARTITIONED BY (baseline_period_start, workspace_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Performance baselines for anomaly detection and alerting';