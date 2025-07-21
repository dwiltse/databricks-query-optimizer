# Genie Space 3: Performance Analytics & Baseline Analysis - Instructions

## Purpose
This Genie Space provides historical performance analysis, trend identification, and baseline establishment to track query performance improvements over time and identify long-term optimization opportunities.

## Key Questions This Space Answers
- "What are the performance trends for queries over the last 3 months?"
- "How has query performance changed since our last optimization?"
- "Which query patterns are getting slower over time?"
- "What's the baseline performance for different types of queries?"
- "Show me the performance impact of recent infrastructure changes"
- "Which workspaces have the best query performance improvements?"

## Data Sources and Relationships

### Core Tables

#### 1. `mcp.query_optimization.mv_daily_performance` (Materialized View)
**What it contains**: Daily aggregated query performance metrics with trending analysis
- **Primary Key**: `date`, `workspace_id`, `performance_category`
- **Key Columns**:
  - `date` - Daily aggregation date
  - `workspace_id` - Workspace identifier
  - `performance_category` - SLOW, MODERATE, FAST (from your business logic)
  - `total_queries` - Total queries executed that day
  - `avg_execution_duration_ms` - Daily average execution time
  - `p50_execution_duration_ms` - 50th percentile execution time
  - `p95_execution_duration_ms` - 95th percentile execution time
  - `total_data_processed_gb` - Total data scanned
  - `avg_bytes_per_row_efficiency` - Average data efficiency
  - `unique_users` - Number of distinct users
  - `error_rate_percent` - Percentage of failed queries
  - `cache_hit_rate_percent` - IO cache effectiveness

#### 2. `mcp.query_optimization.mv_hourly_performance` (Materialized View)
**What it contains**: Hourly performance metrics for detailed trend analysis
- **Primary Key**: `hour_timestamp`, `warehouse_id`
- **Key Columns**:
  - `hour_timestamp` - Hourly time bucket
  - `warehouse_id` - SQL warehouse identifier
  - `query_count` - Queries processed in hour
  - `avg_duration_ms` - Average execution time
  - `slow_query_count` - Queries >300 seconds (your SLOW threshold)
  - `moderate_query_count` - Queries 60-300 seconds (your MODERATE threshold)
  - `fast_query_count` - Queries <60 seconds (your FAST threshold)
  - `total_cpu_hours` - Compute time consumed
  - `total_dbu_consumed` - DBU usage for hour
  - `peak_concurrent_queries` - Maximum concurrent queries
  - `avg_queue_time_ms` - Average waiting time for compute

#### 3. `mcp.query_optimization.performance_baselines` (Table)
**What it contains**: Established performance baselines for different query types and patterns
- **Primary Key**: `baseline_id`
- **Key Columns**:
  - `baseline_type` - Type of baseline (workspace, user, query_pattern, warehouse)
  - `baseline_identifier` - Specific ID (workspace_id, user_id, pattern_hash, warehouse_id)
  - `performance_category` - SLOW, MODERATE, FAST
  - `baseline_duration_ms` - Expected execution time
  - `baseline_efficiency` - Expected bytes_per_row_efficiency
  - `baseline_established_date` - When baseline was set
  - `baseline_period_days` - Period used to calculate baseline (30, 60, 90 days)
  - `confidence_interval_lower` - Lower bound of expected performance
  - `confidence_interval_upper` - Upper bound of expected performance
  - `sample_size` - Number of queries used for baseline
  - `last_validated_date` - When baseline was last checked

#### 4. `mcp.query_optimization.query_performance_raw` (Table)
**What it contains**: Detailed query execution records with performance analysis
- **Primary Key**: `query_id`
- **Key Columns**:
  - `query_hash` - Normalized query identifier
  - `workspace_id` - Workspace where query executed
  - `user_id` - User who executed query
  - `warehouse_id` - SQL warehouse used
  - `query_date` - Date of execution
  - `execution_duration_ms` - Core processing time
  - `total_duration_ms` - Complete execution time
  - `bytes_processed` - Data volume scanned
  - `rows_processed` - Rows processed
  - `performance_score` - Calculated performance rating (1-10)
  - `optimization_opportunities` - Identified improvement areas
  - `baseline_variance_percent` - Performance vs established baseline
  - `created_at` - Record creation timestamp

## Table Relationships and Foreign Keys

### Primary Relationships
```sql
-- Daily aggregations from raw performance
query_performance_raw.query_date → mv_daily_performance.date
query_performance_raw.workspace_id → mv_daily_performance.workspace_id

-- Hourly aggregations from raw performance  
query_performance_raw.query_date → mv_hourly_performance.hour_timestamp
query_performance_raw.warehouse_id → mv_hourly_performance.warehouse_id

-- Baseline relationships
performance_baselines.baseline_identifier → query_performance_raw.workspace_id (when baseline_type = 'workspace')
performance_baselines.baseline_identifier → query_performance_raw.user_id (when baseline_type = 'user')
performance_baselines.baseline_identifier → query_performance_raw.warehouse_id (when baseline_type = 'warehouse')

-- Cross-space relationships
query_performance_raw.query_hash → query_patterns.query_hash (Space 2)
```

### Key Join Patterns
```sql
-- Performance trend analysis with baselines
SELECT 
    d.date, 
    d.avg_execution_duration_ms, 
    b.baseline_duration_ms,
    (d.avg_execution_duration_ms - b.baseline_duration_ms) / b.baseline_duration_ms * 100 as variance_percent
FROM mv_daily_performance d
JOIN performance_baselines b ON d.workspace_id = b.baseline_identifier
WHERE b.baseline_type = 'workspace'

-- Hourly performance with variance analysis
SELECT 
    h.hour_timestamp,
    h.avg_duration_ms,
    r.baseline_variance_percent
FROM mv_hourly_performance h
JOIN query_performance_raw r ON h.warehouse_id = r.warehouse_id
    AND DATE(h.hour_timestamp) = r.query_date
```

## Business Context and Definitions

### Performance Score Scale (1-10)
- **9-10**: Excellent - Consistently fast with optimal resource usage
- **7-8**: Good - Above baseline performance with minor optimization opportunities
- **5-6**: Average - Meeting baseline expectations, room for improvement
- **3-4**: Poor - Below baseline performance, optimization needed
- **1-2**: Critical - Significantly below baseline, immediate attention required

### Baseline Types and Purposes
- **workspace**: Overall workspace performance expectations
- **user**: Individual user's typical query performance
- **query_pattern**: Performance expectations for specific query types
- **warehouse**: Expected performance for specific compute configurations

### Trend Analysis Indicators
- **Improving**: Performance getting better over time (negative variance trend)
- **Degrading**: Performance getting worse over time (positive variance trend)
- **Stable**: Performance staying within baseline confidence intervals
- **Volatile**: Performance varying significantly with no clear trend

### Performance Categories (Consistent with Space 1)
- **SLOW**: execution_duration_ms > 300,000 (5 minutes)
- **MODERATE**: execution_duration_ms between 60,000-300,000 (1-5 minutes)
- **FAST**: execution_duration_ms < 60,000 (1 minute)

## Example Queries for Training

### 1. Performance Trend Analysis Over Time
```sql
SELECT 
    d.date,
    d.workspace_id,
    d.performance_category,
    d.avg_execution_duration_ms,
    b.baseline_duration_ms,
    ROUND((d.avg_execution_duration_ms - b.baseline_duration_ms) / b.baseline_duration_ms * 100, 2) as variance_from_baseline_percent,
    d.total_queries,
    d.error_rate_percent
FROM mcp.query_optimization.mv_daily_performance d
LEFT JOIN mcp.query_optimization.performance_baselines b 
    ON d.workspace_id = CAST(b.baseline_identifier AS BIGINT)
    AND b.baseline_type = 'workspace'
    AND d.performance_category = b.performance_category
WHERE d.date >= CURRENT_DATE - INTERVAL 90 DAY
ORDER BY d.date DESC, d.workspace_id, d.performance_category
```

### 2. Baseline Establishment and Validation
```sql
SELECT 
    b.baseline_type,
    b.baseline_identifier,
    b.performance_category,
    b.baseline_duration_ms,
    b.confidence_interval_lower,
    b.confidence_interval_upper,
    b.sample_size,
    b.baseline_established_date,
    DATEDIFF(CURRENT_DATE, b.last_validated_date) as days_since_validation,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, b.last_validated_date) > 30 THEN 'NEEDS_UPDATE'
        ELSE 'CURRENT'
    END as baseline_status
FROM mcp.query_optimization.performance_baselines b
ORDER BY b.baseline_established_date DESC, b.baseline_type
```

### 3. Performance Improvement Analysis
```sql
WITH recent_performance AS (
    SELECT 
        workspace_id,
        performance_category,
        AVG(avg_execution_duration_ms) as recent_avg_duration,
        AVG(error_rate_percent) as recent_error_rate
    FROM mv_daily_performance 
    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY workspace_id, performance_category
),
historical_performance AS (
    SELECT 
        workspace_id,
        performance_category,
        AVG(avg_execution_duration_ms) as historical_avg_duration,
        AVG(error_rate_percent) as historical_error_rate
    FROM mv_daily_performance 
    WHERE date BETWEEN CURRENT_DATE - INTERVAL 120 DAY AND CURRENT_DATE - INTERVAL 90 DAY
    GROUP BY workspace_id, performance_category
)
SELECT 
    r.workspace_id,
    r.performance_category,
    r.recent_avg_duration,
    h.historical_avg_duration,
    ROUND((r.recent_avg_duration - h.historical_avg_duration) / h.historical_avg_duration * 100, 2) as duration_change_percent,
    r.recent_error_rate,
    h.historical_error_rate,
    ROUND(r.recent_error_rate - h.historical_error_rate, 2) as error_rate_change,
    CASE 
        WHEN r.recent_avg_duration < h.historical_avg_duration THEN 'IMPROVED'
        WHEN r.recent_avg_duration > h.historical_avg_duration * 1.1 THEN 'DEGRADED'
        ELSE 'STABLE'
    END as performance_trend
FROM recent_performance r
JOIN historical_performance h 
    ON r.workspace_id = h.workspace_id 
    AND r.performance_category = h.performance_category
ORDER BY duration_change_percent ASC
```

### 4. Hourly Performance Patterns
```sql
SELECT 
    EXTRACT(HOUR FROM hour_timestamp) as hour_of_day,
    warehouse_id,
    AVG(query_count) as avg_queries_per_hour,
    AVG(avg_duration_ms) as avg_duration_ms,
    AVG(slow_query_count) as avg_slow_queries,
    AVG(total_dbu_consumed) as avg_dbu_per_hour,
    AVG(peak_concurrent_queries) as avg_peak_concurrency
FROM mcp.query_optimization.mv_hourly_performance
WHERE hour_timestamp >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY EXTRACT(HOUR FROM hour_timestamp), warehouse_id
ORDER BY hour_of_day, warehouse_id
```

### 5. Performance Score Distribution Analysis
```sql
SELECT 
    performance_score,
    COUNT(*) as query_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    AVG(execution_duration_ms) as avg_duration,
    AVG(baseline_variance_percent) as avg_baseline_variance
FROM mcp.query_optimization.query_performance_raw
WHERE query_date >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY performance_score
ORDER BY performance_score DESC
```

## Business Instructions for Genie

### Baseline Analysis Guidelines
- **Baseline Establishment**: Use 90-day periods for stable baselines, 30-day for rapidly changing environments
- **Confidence Intervals**: Consider performance within ±20% of baseline as normal variance
- **Baseline Updates**: Refresh baselines monthly or after major infrastructure changes
- **Validation Logic**: Flag baselines as outdated if not validated in >30 days

### Trend Analysis Rules
- **Short-term trends**: Use 7-30 day windows for immediate performance changes
- **Medium-term trends**: Use 30-90 day windows for optimization impact assessment  
- **Long-term trends**: Use 90+ day windows for capacity planning and infrastructure analysis
- **Seasonal patterns**: Consider business cycles when analyzing performance trends

### Performance Scoring Context
- **Score degradation**: Investigate any workspace with >20% of queries scoring <5
- **Improvement tracking**: Celebrate workspaces showing consistent score improvements
- **Comparative analysis**: Use scores to rank workspaces and identify best practices

### Time-Based Analysis Preferences
- **Hourly patterns**: Identify peak usage hours for capacity planning
- **Daily trends**: Track day-over-day performance for optimization impact
- **Weekly patterns**: Understand business cycle impacts on performance
- **Monthly analysis**: Long-term trending for strategic planning

### Variance Analysis Thresholds
- **Normal variance**: ±20% from baseline is typical operational variation
- **Concerning variance**: +20% to +50% indicates potential performance issues
- **Critical variance**: >+50% requires immediate investigation
- **Exceptional performance**: <-20% may indicate successful optimizations

## Data Freshness Expectations
- `mv_daily_performance`: Updated daily at 2 AM UTC with previous day's complete data
- `mv_hourly_performance`: Updated every hour with 2-hour delay for data completeness
- `performance_baselines`: Updated monthly via automated baseline recalculation process
- `query_performance_raw`: Near real-time, populated within 15 minutes of query completion

## Common Analysis Patterns

### Baseline Management
- **Establish baselines**: Use 90-day historical periods for new baseline calculations
- **Validate baselines**: Compare current performance to established baselines monthly
- **Update triggers**: Refresh baselines after major infrastructure changes or optimization initiatives
- **Confidence levels**: Use statistical confidence intervals to define normal variance ranges

### Trend Identification
- **Performance degradation**: Look for consistent increases in execution time over 30+ days
- **Optimization impact**: Compare performance before/after optimization implementations
- **Seasonal patterns**: Identify recurring performance patterns tied to business cycles
- **Infrastructure correlation**: Link performance changes to cluster or warehouse modifications

### Comparative Analysis
- **Workspace benchmarking**: Compare workspaces to identify high-performing teams
- **User performance**: Track individual user performance trends for training opportunities
- **Query pattern evolution**: Monitor how different query types perform over time
- **Resource efficiency**: Track DBU consumption vs performance improvements

## Tips for Effective Queries

1. **Use appropriate time windows**: Match analysis period to question scope (daily for trends, hourly for patterns)
2. **Include baseline context**: Always compare current performance to established baselines
3. **Group by logical dimensions**: Most insights come from grouping by workspace, user, or time period
4. **Calculate variance percentages**: Relative changes are more meaningful than absolute differences
5. **Consider confidence intervals**: Account for normal variance when identifying true performance changes
6. **Filter by performance category**: Different categories (SLOW/MODERATE/FAST) may have different trend patterns

This Genie Space provides comprehensive historical analysis and baseline management to track query performance evolution and identify optimization opportunities over time.