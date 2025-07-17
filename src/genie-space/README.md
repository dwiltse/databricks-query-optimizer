# Databricks Query Optimizer - Genie Spaces

This directory contains documentation and configuration for multiple focused Genie Spaces designed to provide AI-powered query optimization insights.

## Genie Space Architecture

Following Databricks best practices, we use **multiple focused Genie Spaces** (≤5 tables each) rather than one large space:

### 🔍 Genie Space 1: Real-Time Query Monitoring
**Purpose**: Live monitoring of query performance and alerts  
**Focus**: "What queries are running slowly right now?"  
**Tables**: 5 (3 system tables + 2 real-time views)

### 💡 Genie Space 2: Query Optimization Opportunities  
**Purpose**: Identify and prioritize optimization opportunities with ROI analysis  
**Focus**: "What are my top optimization opportunities by potential savings?"  
**Tables**: 5 (4 created tables/views + system.billing.usage)  
**Documentation**: [`genie-space-2-optimization-opportunities/instructions.md`](genie-space-2-optimization-opportunities/instructions.md)

### 📊 Genie Space 3: Performance Analytics
**Purpose**: Historical performance trending and baseline analysis  
**Focus**: "How is our query performance trending over time?"  
**Tables**: 4 (all created tables and materialized views)

## Quick Reference

### Genie Space 2 Tables Summary
```sql
-- Core optimization tables
mcp.query_optimization.query_patterns              -- Detected anti-patterns
mcp.query_optimization.optimization_tracking       -- Implementation tracking
mcp.query_optimization.mv_pattern_performance      -- Pattern metrics (materialized view)
mcp.query_optimization.mv_user_performance         -- User-specific opportunities (materialized view)
system.billing.usage                               -- Cost data for ROI analysis
```

### Key Relationships
```sql
-- Primary joins for optimization analysis
query_patterns.query_hash → optimization_tracking.query_hash
query_patterns.workspace_id → system.billing.usage.workspace_id
mv_user_performance.workspace_id → query_patterns.workspace_id
```

## Configuration

All business rules and thresholds are managed in [`config.yaml`](config.yaml):
- Performance thresholds (slow query = 5+ minutes)
- Cost thresholds (expensive query = 10+ DBUs)
- Optimization scoring (1-10 scale)
- Savings estimates by pattern type

## Example Questions for Each Space

### Genie Space 2: Optimization Opportunities
- "Show me optimization opportunities with >$1000 monthly savings"
- "Which users have the most SELECT * queries to optimize?"
- "What's the ROI of our recent query optimizations?"
- "How much could we save by fixing unbounded sorts?"

## Implementation Guidelines

### Creating New Genie Spaces
1. **Folder Structure**: `genie-space-N-[purpose]/`
2. **Documentation**: Include `instructions.md` with:
   - Table relationships and foreign keys
   - Example queries for training
   - Business context and definitions
3. **Table Limit**: Maximum 5 tables per space
4. **Focused Purpose**: Each space should answer specific question types

### Best Practices
- Start with system tables for real-time data
- Use created tables/materialized views for complex analytics
- Include cost data (system.billing.usage) for ROI analysis
- Provide clear table relationships for effective joins
- Include business context for optimization scoring

## Next Steps

1. **Deploy Genie Space 2** using the tables and instructions provided
2. **Create Genie Space 1** for real-time monitoring
3. **Create Genie Space 3** for performance analytics
4. **Implement agent orchestration** to route questions between spaces

## Related Documentation
- **Main Configuration**: [`config.yaml`](config.yaml)
- **SQL Scripts**: [`sql/`](sql/) directory
- **Orchestration**: [`orchestration/`](orchestration/) directory
- **Validation**: [`validation/`](validation/) directory