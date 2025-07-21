# Genie Space 1: Real-Time Query Performance Monitoring - Instructions

## Purpose
This Genie Space enables real-time monitoring and analysis of SQL query performance across your Databricks workspaces, helping identify performance issues, resource bottlenecks, and optimization opportunities as they occur.

## Key Questions This Space Answers
- "Show me the slowest queries running in the last hour"
- "Which queries are consuming the most resources right now?"
- "What queries failed today and why?"
- "Which users are running the most expensive queries?"
- "Show me queries with poor cache utilization"
- "What's the performance trend for my queries this week?"

## Data Sources and Relationships

### Core Tables

#### 1. `system.query.history` (System Table)
**What it contains**: Complete query execution history with detailed performance metrics
- **Primary Key**: `statement_id`
- **Key Performance Columns**:
  - `statement_text` - Full SQL query text
  - `executed_by` - User who executed the query
  - `total_duration_ms` - Total execution time including waiting
  - `execution_duration_ms` - Actual statement execution time
  - `result_fetch_duration_ms` - Time to fetch results to client
  - `read_bytes` - Total data read
  - `read_rows` - Total rows processed
  - `read_partitions` - Number of partitions scanned
  - `compute.warehouse_id` - SQL warehouse used
  - `error_message` - Failure details if query errored
  - `end_time` - Query completion timestamp

#### 2. `system.compute.clusters` (System Table)
**What it contains**: Compute cluster configurations and metadata
- **Primary Key**: `cluster_id`
- **Key Columns**:
  - `cluster_name` - Human-readable cluster name
  - `worker_count` - Number of worker nodes
  - `driver_node_type` - Driver instance type
  - `worker_node_type` - Worker instance type
  - `dbr_version` - Databricks Runtime version
  - `auto_termination_minutes` - Auto-shutdown configuration

#### 3. `mcp.query_optimization.mv_query_performance_categorized` (Materialized View)
**What it contains**: Pre-categorized query performance analysis based on your business logic
- **Key Columns**:
  - `statement_id` - Links to system.query.history
  - `statement_text` - Query text
  - `executed_by` - User identifier
  - `warehouse_id` - SQL warehouse used
  - `performance_category` - SLOW (>300s), MODERATE (60-300s), FAST (<60s)
  - `bytes_per_row_efficiency` - Data scan efficiency metric
  - `optimization_flag` - ERROR, SLOW_FETCH, HEALTHY status
  - `total_duration_ms` - Complete execution time
  - `execution_duration_ms` - Core processing time
  - `result_fetch_duration_ms` - Client fetch time
  - `end_time` - Completion timestamp

#### 4. `mcp.query_optimization.mv_current_slow_queries` (Materialized View)
**What it contains**: Real-time view of performance issues requiring attention
- **Key Columns**:
  - `statement_id` - Query identifier
  - `executed_by` - User running slow query
  - `warehouse_id` - Compute resource used
  - `execution_duration_ms` - Processing time
  - `read_bytes` - Data volume processed
  - `performance_impact_score` - Severity ranking (1-10)
  - `suggested_optimization` - Recommended improvement
  - `first_seen` - When performance issue was first detected
  - `occurrence_count` - How often this slow pattern occurs

#### 5. `mcp.query_optimization.mv_resource_utilization_alerts` (Materialized View)
**What it contains**: Resource usage alerts and efficiency metrics
- **Key Columns**:
  - `warehouse_id` - SQL warehouse identifier
  - `time_window` - 15-minute aggregation window
  - `avg_cpu_utilization` - Average CPU usage percentage
  - `avg_memory_utilization` - Average memory usage percentage  
  - `query_count` - Queries processed in window
  - `total_data_processed_gb` - Data volume processed
  - `cache_hit_ratio` - IO cache effectiveness
  - `alert_level` - CRITICAL, WARNING, NORMAL
  - `bottleneck_type` - CPU, MEMORY, NETWORK, DISK

## Table Relationships and Foreign Keys

### Primary Relationships
```sql
-- Query to Performance Category relationship
system.query.history.statement_id → mv_query_performance_categorized.statement_id

-- Query to Compute relationship
system.query.history.compute.warehouse_id → system.compute.clusters.cluster_id

-- Performance to Alerts relationship
mv_query_performance_categorized.warehouse_id → mv_resource_utilization_alerts.warehouse_id

-- Slow Queries to Historical Data
mv_current_slow_queries.statement_id → system.query.history.statement_id
```

### Key Join Patterns
```sql
-- Most common join: Query performance with compute context
SELECT q.statement_text, q.performance_category, c.cluster_name, c.worker_count
FROM mcp.query_optimization.mv_query_performance_categorized q
LEFT JOIN system.compute.clusters c ON q.warehouse_id = c.cluster_id

-- Resource utilization with query context
SELECT q.executed_by, q.performance_category, r.alert_level, r.bottleneck_type
FROM mcp.query_optimization.mv_query_performance_categorized q
JOIN mcp.query_optimization.mv_resource_utilization_alerts r 
    ON q.warehouse_id = r.warehouse_id
    AND DATE(q.end_time) = DATE(r.time_window)
```

## Business Context and Definitions

### Performance Categories (Based on Your Business Logic)
- **SLOW**: Queries with execution_duration_ms > 300,000 (5 minutes)
  - *Action Required*: Immediate optimization needed
  - *Expected Impact*: High cost and user experience impact
- **MODERATE**: Queries with execution_duration_ms between 60,000-300,000 (1-5 minutes)
  - *Action Required*: Monitor and optimize when possible
  - *Expected Impact*: Medium cost impact, acceptable performance
- **FAST**: Queries with execution_duration_ms < 60,000 (1 minute)
  - *Action Required*: No immediate action needed
  - *Expected Impact*: Optimal performance and cost efficiency

### Optimization Flags (Based on Your Business Logic)
- **ERROR**: Queries with error_message IS NOT NULL
  - *Priority*: Critical - investigate immediately
  - *Common Causes*: Resource limits, syntax errors, permission issues
- **SLOW_FETCH**: Queries with result_fetch_duration_ms > 30,000 (30 seconds)
  - *Priority*: High - client-side performance issue
  - *Common Causes*: Large result sets, network issues, client configuration
- **HEALTHY**: Normal queries with no performance flags
  - *Priority*: Normal monitoring

### Efficiency Metrics
- **bytes_per_row_efficiency**: Data scan efficiency (lower is better)
  - *Calculation*: read_bytes / read_rows
  - *Good*: <1000 bytes per row
  - *Concerning*: >10000 bytes per row
  - *Critical*: >100000 bytes per row

### Alert Levels
- **CRITICAL**: Resource utilization >90% or multiple slow queries
- **WARNING**: Resource utilization >75% or performance degradation  
- **NORMAL**: Healthy resource usage and query performance

## Example Queries for Training

### 1. Current Performance Issues Requiring Attention
```sql
SELECT 
    executed_by,
    performance_category,
    optimization_flag,
    COUNT(*) as issue_count,
    AVG(execution_duration_ms) as avg_duration_ms,
    MAX(bytes_per_row_efficiency) as worst_efficiency
FROM mcp.query_optimization.mv_query_performance_categorized
WHERE end_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
    AND (performance_category = 'SLOW' OR optimization_flag != 'HEALTHY')
GROUP BY executed_by, performance_category, optimization_flag
ORDER BY issue_count DESC, avg_duration_ms DESC
```

### 2. Resource Utilization Alerts by Warehouse
```sql
SELECT 
    r.warehouse_id,
    c.cluster_name,
    r.alert_level,
    r.bottleneck_type,
    r.avg_cpu_utilization,
    r.avg_memory_utilization,
    r.query_count,
    r.cache_hit_ratio
FROM mcp.query_optimization.mv_resource_utilization_alerts r
LEFT JOIN system.compute.clusters c ON r.warehouse_id = c.cluster_id
WHERE r.time_window >= CURRENT_TIMESTAMP - INTERVAL 2 HOUR
    AND r.alert_level IN ('CRITICAL', 'WARNING')
ORDER BY 
    CASE r.alert_level WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 END,
    r.avg_cpu_utilization DESC
```

### 3. Slowest Queries with User Context
```sql
SELECT 
    q.executed_by,
    q.statement_text,
    q.execution_duration_ms,
    q.bytes_per_row_efficiency,
    q.performance_category,
    s.suggested_optimization,
    c.cluster_name,
    c.worker_count
FROM mcp.query_optimization.mv_query_performance_categorized q
LEFT JOIN mcp.query_optimization.mv_current_slow_queries s 
    ON q.statement_id = s.statement_id
LEFT JOIN system.compute.clusters c ON q.warehouse_id = c.cluster_id
WHERE q.end_time >= CURRENT_DATE - INTERVAL 1 DAY
    AND q.performance_category IN ('SLOW', 'MODERATE')
ORDER BY q.execution_duration_ms DESC
LIMIT 20
```

### 4. Performance Trends Over Time
```sql
SELECT 
    DATE_TRUNC('hour', end_time) as hour_window,
    performance_category,
    COUNT(*) as query_count,
    AVG(execution_duration_ms) as avg_duration,
    AVG(bytes_per_row_efficiency) as avg_efficiency,
    COUNT(DISTINCT executed_by) as unique_users
FROM mcp.query_optimization.mv_query_performance_categorized
WHERE end_time >= CURRENT_DATE - INTERVAL 7 DAY
    AND warehouse_id IS NOT NULL
GROUP BY DATE_TRUNC('hour', end_time), performance_category
ORDER BY hour_window DESC, performance_category
```

## Business Instructions for Genie

### Performance Thresholds (Critical Business Logic)
- Always categorize query performance using our established thresholds:
  - **SLOW queries**: execution time > 5 minutes (300,000 ms)
  - **MODERATE queries**: execution time between 1-5 minutes (60,000-300,000 ms)  
  - **FAST queries**: execution time < 1 minute (60,000 ms)

### Efficiency Analysis Guidelines
- When analyzing data scan efficiency, use bytes_per_row_efficiency metric
- Flag queries with >10,000 bytes per row as inefficient data access
- Always consider both execution time AND data efficiency for complete analysis

### Alert Prioritization Rules
- **ERROR queries**: Highest priority - investigate failed queries first
- **SLOW_FETCH queries**: High priority - client performance issues
- **SLOW queries**: Medium priority - resource optimization needed
- **Resource alerts**: Context-dependent - critical if affecting multiple users

### Time-Based Analysis Preferences
- **Real-time monitoring**: Focus on last 1-2 hours for immediate issues
- **Daily analysis**: Use last 24 hours for trend identification
- **Weekly trends**: Use last 7 days for pattern recognition
- Always filter WHERE warehouse_id IS NOT NULL to focus on SQL warehouse queries

### User-Centric Analysis
- Group performance issues by executed_by to identify users needing support
- Consider query frequency when assessing user impact
- Prioritize optimizations affecting multiple users over single-user issues

## Data Freshness Expectations
- `system.query.history`: Real-time (queries appear within minutes of completion)
- `system.compute.clusters`: Updated when cluster configurations change
- `mv_query_performance_categorized`: Refreshed every 15 minutes with latest query data
- `mv_current_slow_queries`: Refreshed every 15 minutes, focuses on last 24 hours
- `mv_resource_utilization_alerts`: Refreshed every 15 minutes with rolling 2-hour windows

## Common Analysis Patterns

### Immediate Action Items
Look for:
- ERROR queries in the last hour (critical failures)
- Multiple SLOW queries from same user (training opportunity)
- CRITICAL resource alerts (capacity planning needed)
- Queries with very high bytes_per_row_efficiency (data model issues)

### Performance Investigation Workflow
1. **Identify issue**: Start with performance_category and optimization_flag filters
2. **Find patterns**: Group by executed_by or warehouse_id to find common factors  
3. **Analyze efficiency**: Check bytes_per_row_efficiency for data access issues
4. **Check resources**: Join with resource utilization alerts for infrastructure context
5. **Historical context**: Compare with previous time periods for trend analysis

### Resource Planning Insights
- Monitor query_count trends to predict capacity needs
- Track cache_hit_ratio to optimize warehouse configurations
- Use bottleneck_type analysis to guide infrastructure improvements
- Correlate alert_level patterns with business usage cycles

## Tips for Effective Queries

1. **Use recent time filters**: Most monitoring should focus on last 1-24 hours
2. **Combine performance + resource data**: Join query performance with utilization alerts
3. **Group by user**: Many insights come from user-specific analysis
4. **Filter by warehouse**: Different warehouses may have different performance characteristics
5. **Order by impact**: Sort by execution time, query count, or efficiency metrics for prioritization

This Genie Space provides real-time visibility into query performance with actionable categorization based on your established business thresholds and optimization flags.