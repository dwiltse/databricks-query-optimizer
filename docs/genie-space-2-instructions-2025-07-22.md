# Genie Space 2: Optimization Opportunities - User Instructions
**Date**: July 22, 2025  
**Focus**: Query optimization recommendations and ROI analysis ONLY

## Purpose
This Genie Space identifies and prioritizes SQL query optimization opportunities with clear ROI analysis and implementation guidance.

## Available Tables (Optimization Focus Only)

- `mcp.query_optimization.query_patterns` - Optimization patterns and recommendations
- `mcp.query_optimization.optimization_tracking` - Implementation ROI tracking  
- `mcp.query_optimization.query_performance_raw` - Base performance data
- `system.billing.usage` - Cost calculation data

## Key Questions for Optimization Analysis

### **Top Optimization Opportunities**
- "What are my top 5 optimization opportunities by potential savings?"
- "Show me SELECT * queries that could save the most money"
- "Which query patterns have the highest ROI if optimized?"
- "Find unbounded sorts that run frequently"

### **ROI and Cost Analysis**
- "Calculate total potential monthly savings from all identified patterns"
- "Which users have the most optimization opportunities?"
- "Show me optimization impact by pattern type"
- "What's the average savings potential across all patterns?"

### **Implementation Tracking**
- "How many optimizations have we implemented this quarter?"
- "What's our average actual savings vs estimated savings?"
- "Which optimization types have the best success rate?"
- "Show me ROI from completed optimizations"

### **Pattern-Specific Analysis**
- "Find all queries using SELECT * with high execution frequency"
- "Show me cartesian joins that could be optimized"
- "Which queries have ORDER BY without LIMIT?"
- "Find data-inefficient queries reading too many bytes per row"

## Business Logic Reference

### **Optimization Categories & Savings Potential**
- **SELECT_ALL**: 40% improvement potential
- **UNBOUNDED_SORT**: 60% improvement potential  
- **CARTESIAN_JOIN**: 80% improvement potential
- **DATA_INEFFICIENT**: 45% improvement potential
- **HIGH_COST**: 25-35% improvement potential

### **Priority Levels**
- **HIGH**: >$1,000 monthly savings potential
- **MEDIUM**: $500-$999 monthly savings potential
- **LOW**: $100-$499 monthly savings potential

## Example Natural Language Queries

### **1. Quick Wins Analysis**
```
"Show me the top 3 optimization opportunities with the highest estimated monthly savings 
and lowest implementation complexity. Include the specific SQL changes needed."
```

### **2. User-Focused Recommendations**  
```
"Which users have query patterns that could save over $500 per month if optimized? 
Show me the specific recommendations for each user."
```

### **3. Pattern Impact Analysis**
```
"Compare the total potential savings across all optimization categories. Which type 
of optimization would give us the biggest bang for our buck?"
```

### **4. Implementation Planning**
```
"Show me all HIGH priority optimization opportunities that haven't been implemented yet. 
Include estimated effort and expected ROI for project planning."
```

### **5. Success Tracking**
```
"What optimizations have we completed in the last 90 days and what was the actual 
cost savings compared to our estimates?"
```

## Key Fields for Analysis

### **Pattern Analysis**
- `pattern_type` - Category of optimization opportunity
- `optimization_priority` - HIGH/MEDIUM/LOW priority
- `optimization_recommendations` - Specific SQL changes needed
- `estimated_monthly_savings_dbu` - Projected cost reduction

### **ROI Tracking**
- `before_avg_cost_dbu` vs `after_avg_cost_dbu` - Actual cost impact
- `actual_savings_pct` vs `estimated_savings_pct` - Accuracy tracking  
- `implementation_date` - Timeline analysis
- `status` - Implementation progress

### **Cost Calculation**
- `system.billing.usage.usage_quantity` - Actual DBU consumption
- `compute_cost_dbu` - Estimated per-query cost
- Join on `workspace_id` for accurate cost attribution

## Analysis Tips

### **For Maximum Impact**
- Focus on HIGH priority patterns first
- Look for patterns with high `occurrence_count` (frequent execution)
- Prioritize optimizations affecting multiple users
- Consider implementation effort vs savings potential

### **For ROI Analysis**
- Compare estimated vs actual savings to improve future estimates
- Track time to implement different optimization types
- Monitor pattern recurrence after optimization
- Measure user adoption of optimization recommendations

### **For Project Planning**
- Group optimizations by similarity for batch implementation
- Identify users who need training on query best practices
- Plan optimization sprints based on potential impact
- Set realistic savings targets based on historical success rates

This Genie Space provides focused optimization analysis with clear business impact metrics and actionable recommendations for improving query cost efficiency.