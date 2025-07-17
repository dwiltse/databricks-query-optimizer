# Genie Space 2: Query Optimization Opportunities - Instructions

## Purpose
This Genie Space helps identify, prioritize, and track SQL query optimization opportunities to reduce costs and improve performance across your Databricks workspaces.

## Key Questions This Space Answers
- "What are my top optimization opportunities by potential savings?"
- "Which users have the most optimization opportunities?"
- "Show me SELECT * queries that could be optimized"
- "What's the ROI of implementing specific optimizations?"
- "How much could we save by optimizing unbounded sorts?"
- "Which query patterns are costing us the most?"

## Data Sources and Relationships

### Core Tables

#### 1. `mcp.query_optimization.query_patterns`
**What it contains**: Detected anti-patterns in SQL queries with cost impact analysis
- **Primary Key**: `pattern_hash` 
- **Key Columns**:
  - `pattern_type` - Type of optimization opportunity (e.g., 'select_all', 'unbounded_sort', 'cartesian_join')
  - `query_hash` - Links to specific queries in query_performance_raw
  - `workspace_id` - Workspace where pattern was found
  - `avg_cost_dbu` - Average cost per execution of this pattern
  - `execution_count` - How many times this pattern has been executed
  - `last_seen` - Most recent occurrence
  - `estimated_savings_pct` - Potential savings percentage if optimized

#### 2. `mcp.query_optimization.optimization_tracking`
**What it contains**: Historical tracking of optimization implementations and their impact
- **Primary Key**: `tracking_id`
- **Key Columns**:
  - `query_hash` - Links back to original queries and patterns
  - `workspace_id` - Where optimization was implemented
  - `user_id` - Who implemented the optimization
  - `optimization_type` - Type of optimization applied
  - `implementation_date` - When optimization was completed
  - `before_avg_duration_ms` - Performance before optimization
  - `after_avg_duration_ms` - Performance after optimization
  - `before_avg_cost_dbu` - Cost before optimization
  - `after_avg_cost_dbu` - Cost after optimization
  - `actual_savings_pct` - Realized savings percentage
  - `status` - Implementation status ('completed', 'in_progress', 'planned')

#### 3. `mcp.query_optimization.mv_pattern_performance` (Materialized View)
**What it contains**: Pre-aggregated performance metrics by pattern type for fast analysis
- **Key Columns**:
  - `pattern_type` - Optimization opportunity category
  - `workspace_id` - Workspace identifier
  - `total_executions` - Total times this pattern has been executed
  - `total_cost_dbu` - Total DBU cost for this pattern
  - `avg_duration_ms` - Average execution time
  - `potential_monthly_savings` - Estimated monthly savings if optimized
  - `affected_users` - Number of users with this pattern
  - `last_seen` - Most recent occurrence

#### 4. `mcp.query_optimization.mv_user_performance` (Materialized View)
**What it contains**: User-specific optimization opportunities and performance metrics
- **Key Columns**:
  - `user_id` - User identifier
  - `workspace_id` - Workspace identifier
  - `total_queries` - Total queries executed by user
  - `optimization_opportunities` - Number of optimization opportunities
  - `potential_savings_dbu` - Potential DBU savings for this user
  - `avg_optimization_score` - Average optimization score (1-10, lower = more opportunities)
  - `most_common_pattern` - Most frequent anti-pattern for this user
  - `last_query` - Most recent query execution

#### 5. `system.billing.usage` (Raw System Table)
**What it contains**: Databricks billing and usage data for cost calculations
- **Key Columns**:
  - `workspace_id` - Links to our optimization data
  - `sku_name` - Service type (e.g., 'JOBS_COMPUTE', 'SQL_COMPUTE')
  - `usage_start_time` - Billing period start
  - `usage_quantity` - DBU consumption
  - `dollar_price` - Cost in USD
  - `usage_metadata` - Additional context like cluster_id

## Table Relationships and Foreign Keys

### Primary Relationships
```sql
-- Pattern to Performance relationship
query_patterns.query_hash → query_performance_raw.query_hash

-- Pattern to Tracking relationship  
query_patterns.query_hash → optimization_tracking.query_hash
query_patterns.workspace_id → optimization_tracking.workspace_id

-- User Performance to Patterns
mv_user_performance.user_id → query_patterns.user_id (through query_performance_raw)
mv_user_performance.workspace_id → query_patterns.workspace_id

-- Billing Integration
system.billing.usage.workspace_id → query_patterns.workspace_id
system.billing.usage.workspace_id → optimization_tracking.workspace_id
```

### Key Join Patterns
```sql
-- Most common join: Pattern analysis with cost data
SELECT p.pattern_type, p.avg_cost_dbu, b.dollar_price
FROM mcp.query_optimization.query_patterns p
JOIN system.billing.usage b ON p.workspace_id = b.workspace_id

-- Tracking optimization impact
SELECT 
    p.pattern_type,
    t.before_avg_cost_dbu,
    t.after_avg_cost_dbu,
    t.actual_savings_pct
FROM mcp.query_optimization.query_patterns p
JOIN mcp.query_optimization.optimization_tracking t 
    ON p.query_hash = t.query_hash
```

## Business Context and Definitions

### Optimization Score Scale (1-10)
- **9-10**: Excellent - Well-optimized queries, minimal improvement needed
- **7-8**: Good - Minor optimization opportunities 
- **5-6**: Average - Moderate optimization potential
- **3-4**: Poor - Significant optimization needed
- **1-2**: Critical - Major optimization required immediately

### Pattern Types and Expected Savings
- **select_all**: SELECT * usage (30% potential savings)
- **unbounded_sort**: ORDER BY without LIMIT (50% potential savings)
- **cartesian_join**: Missing join conditions (80% potential savings)
- **unpartitioned_filter**: Missing partition filters (40% potential savings)
- **redundant_distinct**: Unnecessary DISTINCT (20% potential savings)
- **union_optimization**: UNION vs UNION ALL (15% potential savings)
- **large_scan_optimization**: Inefficient data access (60% potential savings)

### Priority Thresholds
- **High Priority**: $1,000+ monthly savings potential
- **Medium Priority**: $500-$999 monthly savings potential  
- **Low Priority**: $100-$499 monthly savings potential

## Example Queries for Training

### 1. Top Optimization Opportunities by Savings
```sql
SELECT 
    pattern_type,
    COUNT(*) as opportunity_count,
    SUM(potential_monthly_savings) as total_potential_savings,
    AVG(avg_cost_dbu) as avg_cost_per_execution,
    COUNT(DISTINCT workspace_id) as affected_workspaces
FROM mcp.query_optimization.mv_pattern_performance
WHERE last_seen >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY pattern_type
ORDER BY total_potential_savings DESC
```

### 2. User-Specific Optimization Recommendations
```sql
SELECT 
    u.user_id,
    u.total_queries,
    u.optimization_opportunities,
    u.potential_savings_dbu,
    u.most_common_pattern,
    p.pattern_type,
    p.estimated_savings_pct
FROM mcp.query_optimization.mv_user_performance u
JOIN mcp.query_optimization.query_patterns p 
    ON u.user_id = p.user_id 
    AND u.workspace_id = p.workspace_id
WHERE u.potential_savings_dbu > 10
ORDER BY u.potential_savings_dbu DESC
```

### 3. ROI Analysis for Optimizations
```sql
SELECT 
    t.optimization_type,
    COUNT(*) as implementations,
    AVG(t.actual_savings_pct) as avg_savings_realized,
    SUM(t.before_avg_cost_dbu - t.after_avg_cost_dbu) as total_cost_reduction,
    AVG(DATEDIFF(t.implementation_date, t.created_at)) as avg_implementation_days
FROM mcp.query_optimization.optimization_tracking t
WHERE t.status = 'completed'
    AND t.implementation_date >= CURRENT_DATE - INTERVAL 90 DAY
GROUP BY t.optimization_type
ORDER BY total_cost_reduction DESC
```

### 4. Cost Impact by Workspace
```sql
SELECT 
    p.workspace_id,
    COUNT(DISTINCT p.pattern_type) as pattern_types,
    SUM(p.avg_cost_dbu * p.execution_count) as total_pattern_cost,
    SUM(b.usage_quantity) as total_dbu_usage,
    SUM(b.dollar_price) as total_cost_usd
FROM mcp.query_optimization.query_patterns p
JOIN system.billing.usage b 
    ON p.workspace_id = b.workspace_id
    AND DATE(b.usage_start_time) = DATE(p.last_seen)
WHERE p.last_seen >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY p.workspace_id
ORDER BY total_pattern_cost DESC
```

## Optimization Instructions

### Query Optimization Guidelines
1. **Focus on high-cost patterns first** - Prioritize optimizations with >$500 monthly savings
2. **Consider implementation effort** - Balance savings potential with complexity
3. **Track before/after metrics** - Always measure actual impact vs estimated
4. **Target repeat offenders** - Users with multiple optimization opportunities

### Cost Calculation Methods
- Monthly savings = `(avg_cost_dbu * execution_count * 30) * estimated_savings_pct`
- ROI = `(actual_savings_pct / estimated_savings_pct) * 100`
- Priority score = `potential_monthly_savings / implementation_effort_hours`

### Data Freshness Expectations
- `query_patterns`: Updated hourly with new query analysis
- `optimization_tracking`: Updated when optimizations are implemented
- `mv_pattern_performance`: Refreshed every hour
- `mv_user_performance`: Refreshed every hour
- `system.billing.usage`: Updated daily with billing data

## Common Analysis Patterns

### Identifying Quick Wins
Look for patterns with:
- High execution frequency (>100 executions/week)
- Low complexity optimization (select_all, redundant_distinct)
- High savings percentage (>30%)
- Multiple affected users

### Tracking Implementation Success
Monitor:
- Actual vs estimated savings percentage
- Time to implement optimizations
- User adoption of optimization recommendations
- Reduction in pattern recurrence

### Cost Attribution Analysis
Combine workspace billing data with pattern costs to:
- Calculate true cost impact of optimization opportunities
- Justify optimization investment with concrete ROI
- Track cost reduction trends after optimization implementation

## Tips for Effective Queries

1. **Use date filters** - Most analyses should focus on recent data (last 30-90 days)
2. **Group by pattern_type** - Most valuable insights come from pattern-based analysis
3. **Join with billing data** - Always consider actual costs when prioritizing
4. **Filter by workspace** - Many organizations want workspace-specific insights
5. **Consider user impact** - Optimization opportunities affecting many users are higher priority

This Genie Space is designed to provide actionable insights for query optimization with clear ROI justification and implementation tracking.