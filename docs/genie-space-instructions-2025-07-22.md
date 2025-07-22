# Genie Space Instructions - Query Optimization Analysis
**Date**: July 22, 2025  
**Workspace**: [Your Workspace Name]  
**Schema**: mcp.query_optimization

## Purpose
This Genie Space helps identify, analyze, and prioritize SQL query optimization opportunities across Databricks workspaces using natural language queries.

## Available Data Sources

### **System Tables (Real-time)**
- `system.query.history` - All executed SQL queries (180 days)
- `system.billing.usage` - DBU consumption and costs (365 days)
- `system.compute.clusters` - Cluster configurations and performance

### **Analytics Tables (Daily Refresh)**
- `mcp.query_optimization.query_performance_raw` - Enriched query performance data
- `mcp.query_optimization.query_patterns` - Identified optimization patterns
- `mcp.query_optimization.optimization_tracking` - Implementation ROI tracking
- `mcp.query_optimization.performance_baselines` - Historical performance baselines

## Key Questions This Genie Space Can Answer

### **Performance Analysis**
- "Show me the 10 slowest queries from last week"
- "What queries are taking longer than 5 minutes to execute?"
- "Which users are running the most expensive queries?"
- "Find queries that scan more than 1GB of data"

### **Cost Optimization**
- "What are our highest cost queries by DBU consumption?"
- "Show me queries that could save money if optimized"
- "Which query patterns have the biggest cost impact?"
- "Calculate potential monthly savings from query optimization"

### **Anti-Pattern Detection**
- "Find queries using SELECT * that could be optimized"
- "Show me queries with ORDER BY but no LIMIT clause"
- "Identify queries with potential cartesian joins"
- "Which queries are reading too many bytes per row?"

### **Trend Analysis**
- "How has query performance changed over the last month?"
- "Show me query volume trends by user"
- "What's our query success rate by day?"
- "Compare this week's performance to last week"

### **Optimization Tracking**
- "Show me the ROI from our recent query optimizations"
- "Which optimization recommendations have been implemented?"
- "What's the success rate of our query optimization efforts?"

## Business Logic Reference

### **Performance Categories**
- **SLOW**: Execution time > 5 minutes (300,000ms)
- **MODERATE**: Execution time 1-5 minutes (60,000-300,000ms)  
- **FAST**: Execution time < 1 minute (<60,000ms)

### **Optimization Priority**
- **HIGH**: Monthly cost impact >$1,000 OR execution time >5 minutes
- **MEDIUM**: Monthly cost impact $500-$999 OR execution time 1-5 minutes
- **LOW**: Monthly cost impact <$500 OR execution time <1 minute

### **Common Anti-Patterns**
- **SELECT_ALL**: Queries using SELECT * (40% optimization potential)
- **UNBOUNDED_SORT**: ORDER BY without LIMIT (60% optimization potential)
- **CARTESIAN_JOIN**: Missing JOIN conditions (80% optimization potential)
- **DATA_INEFFICIENT**: >100KB bytes per row read (45% optimization potential)

## Example Queries for Training

### **1. Find Worst Performing Queries**
```
Show me the top 5 queries with the highest performance impact score from the last 30 days, 
including their execution time, data read, and optimization recommendations.
```

### **2. Cost Impact Analysis**
```
What queries are costing us the most in DBUs per month? Show me the top 10 with estimated 
cost and potential savings if optimized.
```

### **3. User-Specific Analysis**
```
Which users are running the most inefficient queries? Show me users with queries that 
have low optimization scores and high resource consumption.
```

### **4. Pattern-Based Optimization**
```
Find all queries using SELECT * that run more than once per day. Calculate the potential 
savings if we optimize these queries by selecting specific columns.
```

### **5. Trend Analysis**
```
How has our average query execution time changed week over week for the last 8 weeks? 
Show me the trend broken down by query complexity.
```

### **6. ROI Tracking**
```
Show me the before and after performance metrics for queries we've optimized in the last 
3 months. What's our average improvement percentage?
```

## Data Relationships

### **Key Joins**
- `query_performance_raw.query_hash` → `query_patterns.query_hash`
- `query_performance_raw.query_id` → `optimization_tracking.query_hash`
- `system.query.history.workspace_id` → `system.billing.usage.workspace_id`
- `query_performance_raw.user_id` → User analysis groupings

### **Important Fields**
- **statement_id**: Unique query execution identifier
- **query_hash**: Pattern matching across similar queries
- **performance_impact_score**: 0-100 scale (higher = worse performance)
- **optimization_score**: 1-10 scale (higher = better optimized)
- **estimated_monthly_savings_dbu**: Potential cost reduction

## Current Limitations and Workarounds

### **Real-time Updates**
- **Limitation**: Automated refresh has technical issues
- **Workaround**: Data refreshed daily via manual process
- **For latest data**: Query `system.query.history` directly

### **Pattern Recognition**
- **Limitation**: Pattern detection is rule-based, not ML-powered
- **Workaround**: Use clear anti-pattern categories for analysis
- **Enhancement**: Future MCP integration will add AI-powered analysis

### **Cost Attribution**
- **Limitation**: Exact cost attribution requires billing table joins
- **Workaround**: Use estimated DBU costs based on execution time
- **Note**: Actual costs may vary based on cluster configuration

## Tips for Effective Analysis

### **Performance Questions**
- Filter by time ranges (last 7 days, last month) for relevant analysis
- Focus on queries with execution_duration_ms > 60000 for optimization impact
- Group by user_email or workspace_id for targeted improvements

### **Cost Questions**
- Sort by estimated_monthly_dbu_cost for highest impact optimizations
- Consider frequency (query patterns that run often) for total cost impact
- Use optimization_priority field to focus efforts

### **Trend Questions**
- Use DATE functions on start_time/end_time for time-based analysis
- Compare performance_impact_score over time periods
- Group by optimization_category to track pattern improvements

This Genie Space provides comprehensive query optimization analysis with clear business impact metrics and actionable recommendations for improving Databricks query performance and cost efficiency.