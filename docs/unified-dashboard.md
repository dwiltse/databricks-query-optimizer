# Unified Dashboard Architecture

## Overview
This document describes the unified dashboard that combines your existing **cost optimization** capabilities with the new **query performance optimization** features.

## Dashboard Structure

### 🎯 Executive Summary Tab
**Data Sources**: Both cost_optimization and query_optimization schemas
**Purpose**: High-level metrics for leadership

#### Key Metrics
- **Total Monthly Cost**: From `mcp.cost_optimization.daily_cost_performance`
- **Query Performance Score**: From `mcp.query_optimization.mv_user_performance`
- **Optimization Opportunities**: Combined from both schemas
- **Realized Savings**: From `mcp.query_optimization.optimization_tracking`

#### Visualizations
- Cost vs. Performance trend line
- ROI from optimization implementations
- Executive scorecard with key KPIs

### 💰 Cost Optimization Tab (Your Existing Dashboard)
**Data Sources**: `mcp.cost_optimization.*` tables
**Purpose**: Cost tracking and financial optimization

#### Existing Queries (Keep As-Is)
- Daily cost overview
- Query efficiency analysis
- Resource utilization metrics
- Top cost drivers by user

#### New Enhancements
- Link to query performance details
- Optimization impact tracking
- Cost savings from performance improvements

### ⚡ Query Performance Tab (New Dashboard)
**Data Sources**: `mcp.query_optimization.*` tables
**Purpose**: Query performance monitoring and optimization

#### Dashboard Sections
1. **Performance Overview**
   - Query volume trends
   - Average execution time
   - Success rate metrics
   - Performance distribution

2. **Real-time Alerts**
   - Slow query alerts
   - Expensive query alerts
   - Query failures
   - Performance anomalies

3. **Optimization Opportunities**
   - Pattern-based recommendations
   - Quick wins identification
   - User-specific guidance
   - Impact estimates

### 🔧 Optimization Recommendations Tab
**Data Sources**: Both schemas combined
**Purpose**: Actionable optimization guidance

#### Unified Recommendations
- **Cost + Performance**: Queries that are both expensive and slow
- **Quick Wins**: Low effort, high impact optimizations
- **Strategic Improvements**: Long-term optimization roadmap
- **User Coaching**: Personalized recommendations

## Integration Points

### 🔄 Data Flow Between Schemas
```sql
-- Example: Cost impact of performance optimizations
SELECT 
    co.user_name,
    co.total_cost,
    qo.avg_optimization_score,
    qo.optimization_opportunity_score,
    -- Calculate potential cost savings from performance optimization
    co.total_cost * (10 - qo.avg_optimization_score) / 10 * 0.3 as potential_cost_savings
FROM mcp.cost_optimization.user_cost_attribution co
JOIN mcp.query_optimization.mv_user_performance qo 
    ON co.user_name = qo.user_id
WHERE co.date >= current_date() - INTERVAL 7 DAYS
    AND qo.avg_optimization_score < 7;
```

### 📊 Cross-Schema Queries

#### 1. Cost-Performance Correlation
```sql
-- Identify users with high cost AND poor performance
SELECT 
    co.user_name,
    co.total_cost,
    co.avg_query_cost,
    qo.avg_duration_ms,
    qo.avg_optimization_score,
    qo.optimization_opportunity_score,
    -- Combined priority score
    (co.total_cost / 100) + (10 - qo.avg_optimization_score) + qo.optimization_opportunity_score as priority_score
FROM mcp.cost_optimization.user_cost_attribution co
JOIN mcp.query_optimization.mv_user_performance qo ON co.user_name = qo.user_id
WHERE co.date >= current_date() - INTERVAL 7 DAYS
ORDER BY priority_score DESC;
```

#### 2. Optimization Impact on Cost
```sql
-- Track how performance optimizations affect cost
SELECT 
    ot.optimization_type,
    ot.optimization_description,
    AVG(ot.cost_improvement_pct) as avg_cost_improvement,
    SUM(ot.actual_monthly_savings_usd) as total_cost_savings,
    -- Correlate with cost optimization metrics
    AVG(co.cost_usd) as avg_user_cost_before,
    AVG(co.cost_usd * (1 - ot.cost_improvement_pct/100)) as estimated_cost_after
FROM mcp.query_optimization.optimization_tracking ot
JOIN mcp.cost_optimization.user_cost_attribution co 
    ON ot.user_id = co.user_name
WHERE ot.implementation_date >= current_date() - INTERVAL 30 DAYS
    AND co.date >= current_date() - INTERVAL 30 DAYS
GROUP BY ot.optimization_type, ot.optimization_description
ORDER BY total_cost_savings DESC;
```

#### 3. Resource Efficiency Score
```sql
-- Combined efficiency score using both cost and performance data
SELECT 
    co.workspace_id,
    co.workspace_name,
    co.efficiency_score as cost_efficiency,
    qo.avg_optimization_score as performance_efficiency,
    -- Combined efficiency score (1-10)
    (co.efficiency_score + qo.avg_optimization_score) / 2 as combined_efficiency_score,
    -- Recommendations based on combined score
    CASE 
        WHEN co.efficiency_score < 0.5 AND qo.avg_optimization_score < 5 THEN 'CRITICAL: Both cost and performance need immediate attention'
        WHEN co.efficiency_score < 0.5 THEN 'HIGH: Focus on cost optimization'
        WHEN qo.avg_optimization_score < 5 THEN 'HIGH: Focus on performance optimization'
        WHEN (co.efficiency_score + qo.avg_optimization_score) / 2 < 6 THEN 'MEDIUM: General optimization recommended'
        ELSE 'LOW: Well optimized workspace'
    END as optimization_priority
FROM mcp.cost_optimization.daily_cost_performance co
JOIN mcp.query_optimization.mv_daily_performance qo 
    ON co.workspace_id = qo.workspace_id
WHERE co.date >= current_date() - INTERVAL 7 DAYS
    AND qo.query_date >= current_date() - INTERVAL 7 DAYS
GROUP BY co.workspace_id, co.workspace_name, co.efficiency_score, qo.avg_optimization_score
ORDER BY combined_efficiency_score ASC;
```

## Implementation Approach

### Phase 1: Keep Your Existing Dashboard
- **No changes** to your current cost optimization dashboard
- Continue using your existing Genie Space setup
- Maintain all current functionality

### Phase 2: Add Query Performance Dashboard
- Deploy new `mcp.query_optimization` schema
- Create new dashboard tab for query performance
- Implement real-time alerting

### Phase 3: Unified Experience
- Add cross-schema queries
- Create unified recommendation engine
- Implement executive summary dashboard

## Dashboard Specifications

### 🎨 Design Patterns
Following your existing dashboard design:
- **Refresh Schedule**: Daily at 6 AM UTC (matching your current schedule)
- **Data Retention**: 90 days for detailed data, 1 year for summaries
- **Clustering**: Optimized for date and workspace_id queries
- **Performance**: Sub-second response times for all dashboard queries

### 📈 Visualization Types
- **Cost Trends**: Line charts with forecasting
- **Performance Metrics**: Gauges and sparklines
- **Alert Status**: Color-coded status indicators
- **Optimization Impact**: Before/after comparison charts
- **User Rankings**: Leaderboards with drill-down capability

### 🔔 Alerting Integration
- **Cost Alerts**: Your existing cost threshold alerts
- **Performance Alerts**: New real-time performance alerts
- **Combined Alerts**: Queries that are both expensive and slow
- **Escalation Rules**: Automated alert routing based on severity

## Sample Dashboard Queries

### Executive Summary Widget
```sql
-- Executive dashboard summary
SELECT 
    'EXECUTIVE_SUMMARY' as widget_type,
    SUM(co.cost_usd) as total_cost_usd,
    COUNT(DISTINCT co.user_name) as active_users,
    AVG(qo.avg_optimization_score) as avg_optimization_score,
    COUNT(CASE WHEN qo.optimization_opportunity_score > 7 THEN 1 END) as high_opportunity_users,
    SUM(ot.actual_monthly_savings_usd) as realized_savings_usd
FROM mcp.cost_optimization.daily_cost_performance co
LEFT JOIN mcp.query_optimization.mv_user_performance qo ON co.user_name = qo.user_id
LEFT JOIN mcp.query_optimization.optimization_tracking ot ON co.user_name = ot.user_id
WHERE co.date >= current_date() - INTERVAL 7 DAYS
    AND ot.implementation_date >= current_date() - INTERVAL 30 DAYS;
```

### Cost-Performance Correlation Widget
```sql
-- Cost vs Performance correlation for trending
SELECT 
    co.date,
    SUM(co.cost_usd) as daily_cost,
    AVG(qo.avg_duration_ms) as avg_query_duration,
    AVG(qo.avg_optimization_score) as avg_optimization_score,
    COUNT(CASE WHEN qo.optimization_opportunity_score > 7 THEN 1 END) as optimization_opportunities
FROM mcp.cost_optimization.daily_cost_performance co
JOIN mcp.query_optimization.mv_daily_performance qo 
    ON co.date = qo.query_date AND co.workspace_id = qo.workspace_id
WHERE co.date >= current_date() - INTERVAL 30 DAYS
GROUP BY co.date
ORDER BY co.date;
```

### Optimization ROI Widget
```sql
-- ROI tracking for optimization implementations
SELECT 
    ot.optimization_type,
    COUNT(*) as implementations,
    AVG(ot.cost_improvement_pct) as avg_cost_improvement,
    SUM(ot.actual_monthly_savings_usd) as total_monthly_savings,
    AVG(ot.duration_improvement_pct) as avg_performance_improvement,
    ROUND(SUM(ot.actual_monthly_savings_usd) / COUNT(*), 2) as avg_savings_per_optimization
FROM mcp.query_optimization.optimization_tracking ot
WHERE ot.implementation_date >= current_date() - INTERVAL 90 DAYS
    AND ot.status = 'VERIFIED'
GROUP BY ot.optimization_type
ORDER BY total_monthly_savings DESC;
```

## Implementation Timeline

### Week 1: Schema Deployment
- Deploy query optimization schema
- Set up ETL pipeline
- Test data integration

### Week 2: Basic Dashboard
- Create query performance dashboard
- Implement real-time alerts
- Test with sample data

### Week 3: Integration
- Add cross-schema queries
- Create unified recommendation engine
- Test integration points

### Week 4: Unified Experience
- Deploy executive summary dashboard
- Implement advanced visualizations
- Train users on new features

## Success Metrics

### Dashboard Adoption
- **User Engagement**: Daily active users of new dashboard
- **Feature Usage**: Most used optimization recommendations
- **Time to Insight**: Average time to identify optimization opportunities

### Business Impact
- **Cost Reduction**: Measurable cost savings from optimizations
- **Performance Improvement**: Query duration improvements
- **User Satisfaction**: Feedback on recommendation quality

### Technical Performance
- **Dashboard Response Time**: <3 seconds for all queries
- **Data Freshness**: <2 hours lag from real-time
- **System Reliability**: >99.9% uptime

This unified dashboard approach maximizes the value of your existing cost optimization investment while adding powerful query performance capabilities, creating a comprehensive platform for Databricks optimization.