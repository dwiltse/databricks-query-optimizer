# Databricks Query Optimization Genie Space Setup
**Date**: July 22, 2025  
**Status**: POC Ready - Post Script Troubleshooting

## Executive Summary

This Genie Space enables natural language queries to identify, prioritize, and optimize SQL query performance issues across Databricks workspaces. The setup includes both system tables and derived analytics tables for comprehensive performance analysis.

## Tables Required for Genie Space

### **Raw System Tables (Direct Access)**
- `system.query.history` - All SQL queries executed (180 days retention)
- `system.billing.usage` - DBU consumption and cost data (365 days retention)
- `system.compute.clusters` - Cluster configuration and performance (365 days retention)

### **Derived Analytics Tables (mcp.query_optimization schema)**

#### **Core Data Layer**
1. **`query_performance_raw`**
   - **Source**: `system.query.history` (via populate_core_tables.sql)
   - **Purpose**: Cleaned and enriched query data with complexity scores
   - **Key Fields**: statement_id, query_text, duration_ms, read_bytes, read_rows, complexity_score, optimization_score
   - **Refresh**: Daily via populate_core_tables.sql

2. **`query_patterns`**
   - **Source**: `query_performance_raw` (via populate_core_tables.sql)
   - **Purpose**: Identified anti-patterns and optimization opportunities
   - **Key Fields**: query_hash, pattern_type, optimization_priority, optimization_recommendations
   - **Refresh**: Daily via populate_core_tables.sql

3. **`optimization_tracking`**
   - **Source**: Manual population when optimizations are implemented
   - **Purpose**: Track ROI and success of optimization efforts
   - **Key Fields**: before/after metrics, actual_savings_pct, implementation_status

4. **`performance_baselines`**
   - **Source**: `query_performance_raw` (via populate_core_tables.sql)
   - **Purpose**: Historical performance baselines for anomaly detection
   - **Key Fields**: baseline metrics, threshold values by query pattern

#### **Analytics Layer (Currently Manual - Real-time Automation Had Issues)**
5. **Direct Query Analysis**
   - **Source**: Use `top_3_worst_queries_analysis.sql` for ad-hoc analysis
   - **Purpose**: Identify worst performing queries with actionable recommendations
   - **Key Output**: Top 3 queries ranked by performance impact score

## Genie Space Configuration

### **Recommended Tables for Initial Genie Space**
Based on Databricks best practice of ≤5 tables per Genie Space:

1. `system.query.history` (raw data)
2. `mcp.query_optimization.query_performance_raw` (enriched data)
3. `mcp.query_optimization.query_patterns` (optimization opportunities)
4. `mcp.query_optimization.optimization_tracking` (ROI tracking)
5. `system.billing.usage` (cost context)

### **Alternative Lightweight Setup**
For rapid POC deployment:
1. `system.query.history` (raw data)
2. `mcp.query_optimization.query_performance_raw` (enriched data)
3. Direct SQL analysis via `top_3_worst_queries_analysis.sql`

## Business Logic and Thresholds

### **Query Performance Categories**
- **SLOW**: execution_duration_ms > 300,000 (5+ minutes)
- **MODERATE**: execution_duration_ms > 60,000 (1-5 minutes)
- **FAST**: execution_duration_ms ≤ 60,000 (<1 minute)

### **Optimization Priority Scoring**
- **HIGH**: Queries >5 minutes OR >$1000 monthly impact
- **MEDIUM**: Queries 1-5 minutes OR $500-$999 monthly impact
- **LOW**: Queries <1 minute OR <$500 monthly impact

### **Performance Impact Score Calculation (0-100)**
- **Duration Impact (40 points)**: Execution time scaled against 5-minute baseline
- **Data Inefficiency (30 points)**: Bytes per row ratio (>100KB/row = penalty)
- **Anti-Pattern Penalties (30 points)**: SELECT *, unbounded sorts, cartesian joins

### **Estimated Savings Potential**
- **SELECT * queries**: 40% improvement potential
- **Unbounded sorts**: 60% improvement potential
- **Cartesian joins**: 80% improvement potential
- **Data inefficient**: 45% improvement potential
- **General slow queries**: 25-35% improvement potential

## Implementation Status

### **✅ Working Components**
- Core table creation (01_schema_setup.sql, 02_core_tables.sql, 03_performance_tables.sql)
- Base data population (populate_core_tables.sql)
- Query analysis (top_3_worst_queries_analysis.sql)

### **⚠️ Components with Issues**
- Real-time refresh automation (refresh_performance_tables.py) - has NULL comparison and timestamp errors
- Automated performance alerting
- Incremental data processing

### **Workaround for POC**
Use manual analysis approach:
1. Run populate_core_tables.sql daily
2. Use top_3_worst_queries_analysis.sql for worst query identification
3. Manual Genie Space queries against core tables
4. Build MCP integration proof-of-concept on working foundation

## Next Steps for Production

1. **Resolve automation issues** in refresh_performance_tables.py
2. **Implement Delta Live Tables** for production data pipeline
3. **Add liquid clustering** for performance optimization
4. **Set up automated scheduling** for data refresh
5. **Create alerting** for performance degradation
6. **Build comprehensive testing** framework

## Files Changed Since Last Version

- Updated populate_core_tables.sql with correct system.query.history field names
- Created top_3_worst_queries_analysis.sql for manual worst query analysis  
- Fixed timestamp and DEFAULT syntax issues across all scripts
- Updated instructions to reflect manual vs automated approach for POC