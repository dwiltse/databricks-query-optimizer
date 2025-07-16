# Query Optimization Examples and Dashboard Concepts

## Sample Queries for Query Performance Analysis

### 1. Top Long-Running Queries
```sql
-- Identify queries that consistently take longer than expected
SELECT 
    query_id,
    user_id,
    workspace_id,
    LEFT(query_text, 100) as query_preview,
    duration_ms,
    start_time,
    bytes_read,
    compute_cost_dbu,
    RANK() OVER (ORDER BY duration_ms DESC) as duration_rank
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 7 DAYS
    AND execution_status = 'FINISHED'
    AND duration_ms > 60000  -- Queries longer than 1 minute
ORDER BY duration_ms DESC
LIMIT 50;
```

### 2. Resource Intensive Queries
```sql
-- Find queries consuming excessive resources
WITH query_stats AS (
    SELECT 
        query_id,
        user_id,
        workspace_id,
        duration_ms,
        bytes_read,
        compute_cost_dbu,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) OVER () as duration_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY bytes_read) OVER () as bytes_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY compute_cost_dbu) OVER () as cost_p95
    FROM system.query.history
    WHERE start_time >= current_date() - INTERVAL 30 DAYS
        AND execution_status = 'FINISHED'
)
SELECT 
    query_id,
    user_id,
    workspace_id,
    duration_ms,
    bytes_read,
    compute_cost_dbu,
    CASE 
        WHEN duration_ms > duration_p95 THEN 'High Duration'
        WHEN bytes_read > bytes_p95 THEN 'High Data Scan'
        WHEN compute_cost_dbu > cost_p95 THEN 'High Cost'
        ELSE 'Multiple Issues'
    END as optimization_category
FROM query_stats
WHERE duration_ms > duration_p95 
    OR bytes_read > bytes_p95 
    OR compute_cost_dbu > cost_p95
ORDER BY compute_cost_dbu DESC;
```

### 3. Query Pattern Analysis
```sql
-- Analyze query patterns for optimization opportunities
SELECT 
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'SELECT_ALL'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
        WHEN UPPER(query_text) LIKE '%JOIN%' AND UPPER(query_text) NOT LIKE '%ON%' THEN 'CARTESIAN_JOIN'
        WHEN UPPER(query_text) LIKE '%WHERE%' AND UPPER(query_text) NOT LIKE '%PARTITION%' THEN 'UNPARTITIONED_FILTER'
        ELSE 'OTHER'
    END as query_pattern,
    COUNT(*) as query_count,
    AVG(duration_ms) as avg_duration_ms,
    AVG(bytes_read) as avg_bytes_read,
    SUM(compute_cost_dbu) as total_cost_dbu
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 14 DAYS
    AND execution_status = 'FINISHED'
GROUP BY 1
ORDER BY total_cost_dbu DESC;
```

### 4. Cost Attribution by User and Workspace
```sql
-- Track cost attribution for optimization recommendations
SELECT 
    workspace_id,
    user_id,
    DATE(start_time) as query_date,
    COUNT(*) as query_count,
    SUM(duration_ms) as total_duration_ms,
    SUM(bytes_read) as total_bytes_read,
    SUM(compute_cost_dbu) as total_cost_dbu,
    AVG(duration_ms) as avg_duration_ms
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 30 DAYS
    AND execution_status = 'FINISHED'
GROUP BY 1, 2, 3
ORDER BY total_cost_dbu DESC;
```

## Dashboard Concepts

### 1. Performance Overview Dashboard
**Key Metrics:**
- Total queries executed (last 24h, 7d, 30d)
- Average query duration
- 95th percentile query duration
- Query success rate
- Top 10 most expensive queries

**Visualizations:**
- Time series chart of query volume and performance
- Heatmap of query performance by hour of day
- Bar chart of top resource-consuming queries
- Pie chart of query status distribution

### 2. Cost Optimization Dashboard
**Key Metrics:**
- Total DBU consumption
- Cost per query
- Cost trends over time
- Optimization opportunity value
- Savings potential

**Visualizations:**
- Cost trend line with forecasting
- Cost breakdown by workspace/user
- Optimization opportunity funnel
- Before/after optimization comparison

### 3. Query Health Dashboard
**Key Metrics:**
- Query failure rate
- Average cluster startup time
- Cache hit ratio
- Data scan efficiency
- Parallelization effectiveness

**Visualizations:**
- Health score gauge
- Failure rate trend
- Performance distribution histogram
- Efficiency metrics radar chart

## Customer App Features

### 1. Query Recommendation Engine
```python
# Example recommendation logic
def analyze_query_performance(query_id):
    recommendations = []
    
    # Check for SELECT * usage
    if "SELECT *" in query_text.upper():
        recommendations.append({
            "type": "SELECT_OPTIMIZATION",
            "message": "Consider selecting only required columns",
            "impact": "Medium",
            "effort": "Low"
        })
    
    # Check for missing LIMIT clauses
    if "ORDER BY" in query_text.upper() and "LIMIT" not in query_text.upper():
        recommendations.append({
            "type": "LIMIT_OPTIMIZATION",
            "message": "Add LIMIT clause to bounded sorts",
            "impact": "High",
            "effort": "Low"
        })
    
    return recommendations
```

### 2. Alert Configuration
```sql
-- Alert for queries exceeding thresholds
CREATE OR REPLACE VIEW query_performance_alerts AS
SELECT 
    query_id,
    user_id,
    workspace_id,
    duration_ms,
    'LONG_RUNNING_QUERY' as alert_type,
    'Query exceeded 5 minute threshold' as alert_message,
    current_timestamp() as alert_timestamp
FROM system.query.history
WHERE duration_ms > 300000  -- 5 minutes
    AND start_time >= current_timestamp() - INTERVAL 1 HOUR;
```

### 3. Optimization Tracking
```sql
-- Track optimization implementation and impact
CREATE TABLE optimization_tracking (
    optimization_id STRING,
    query_id STRING,
    user_id STRING,
    workspace_id STRING,
    recommendation_type STRING,
    implementation_date DATE,
    before_duration_ms BIGINT,
    after_duration_ms BIGINT,
    before_cost_dbu DECIMAL(10,4),
    after_cost_dbu DECIMAL(10,4),
    savings_dbu DECIMAL(10,4),
    status STRING
);
```

## Implementation Patterns

### 1. Modular Dashboard Design
- Configurable widgets based on user role
- Drill-down capabilities from high-level metrics
- Contextual filtering (workspace, time range, user)
- Real-time vs. historical view toggles

### 2. Query Classification System
- Automatic categorization of query types
- Performance baseline establishment
- Anomaly detection for performance regression
- Pattern-based optimization suggestions

### 3. Cost Forecasting
- Historical trend analysis
- Seasonal adjustment factors
- Growth projection models
- Budget variance alerts

## TODO: Feature Implementation
- [ ] Implement real-time query monitoring
- [ ] Build recommendation engine with ML models
- [ ] Create automated alert system
- [ ] Develop cost forecasting algorithms
- [ ] Build optimization impact tracking
- [ ] Create custom dashboard builder
- [ ] Implement query performance baselines