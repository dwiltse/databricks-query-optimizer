# Genie Space 2: Query Optimization Opportunities Setup
**Date**: July 22, 2025  
**Focus**: Optimization recommendations and ROI analysis ONLY

## Executive Summary

Genie Space 2 focuses exclusively on identifying and tracking SQL query optimization opportunities with ROI analysis. This is a subset of the full query optimization platform, designed for targeted optimization recommendations.

## Tables Required for Genie Space 2 ONLY

### **Core Tables (≤5 for Databricks best practice)**

1. **`mcp.query_optimization.query_patterns`**
   - **Purpose**: Detected anti-patterns with cost impact analysis
   - **Key Fields**: pattern_type, optimization_priority, optimization_recommendations, estimated_monthly_savings_dbu
   - **Source**: populate_core_tables.sql

2. **`mcp.query_optimization.optimization_tracking`**
   - **Purpose**: Track ROI of implemented optimizations
   - **Key Fields**: before/after metrics, actual_savings_pct, implementation_status
   - **Source**: Manual when optimizations implemented

3. **`mcp.query_optimization.query_performance_raw`**
   - **Purpose**: Base query data for pattern analysis
   - **Key Fields**: query_hash, optimization_score, complexity_score, compute_cost_dbu
   - **Source**: populate_core_tables.sql

4. **`system.billing.usage`**
   - **Purpose**: Real cost data for ROI calculations
   - **Key Fields**: usage_quantity, workspace_id, usage_start_time
   - **Source**: System table (direct access)

5. **Manual Analysis via `top_3_worst_queries_analysis.sql`**
   - **Purpose**: Ad-hoc worst query identification
   - **Output**: Top optimization opportunities with deployment steps

## Business Logic for Optimization Focus

### **Optimization Categories**
- **SELECT_ALL**: SELECT * usage (40% potential savings)
- **UNBOUNDED_SORT**: ORDER BY without LIMIT (60% potential savings)  
- **CARTESIAN_JOIN**: Missing join conditions (80% potential savings)
- **DATA_INEFFICIENT**: High bytes per row (45% potential savings)
- **HIGH_COST**: Expensive queries (25-35% potential savings)

### **Priority Thresholds**
- **HIGH**: >$1,000 monthly savings potential
- **MEDIUM**: $500-$999 monthly savings potential
- **LOW**: $100-$499 monthly savings potential

## What's NOT Included (Other Genie Spaces)

❌ Real-time monitoring and alerts (Genie Space 1)  
❌ Historical performance trending (Genie Space 3)  
❌ Automated refresh pipelines (infrastructure focus)  
❌ General query analysis (focus is optimization only)

## Setup Steps for Genie Space 2

### **1. Create Tables**
```sql
-- Run these in order:
-- 01_schema_setup.sql
-- 02_core_tables.sql  
-- 03_performance_tables.sql
```

### **2. Populate Base Data**
```sql
-- Run once for initial data:
-- populate_core_tables.sql
```

### **3. Create Genie Space**
- Include only the 4 core tables listed above
- Focus on optimization patterns and ROI tracking
- Use top_3_worst_queries_analysis.sql for analysis

### **4. Key Metrics to Track**
- Total potential monthly savings (DBUs)
- Optimization opportunities by category
- Implementation success rate
- Actual vs estimated savings

## Current Status - POC Ready

✅ **Working**: Core table population and pattern identification  
✅ **Working**: Manual worst query analysis  
✅ **Working**: ROI calculation framework  
⚠️ **Pending**: Automated refresh (use manual approach for POC)

This focused scope gives you everything needed for Genie Space 2 without the complexity of the full monitoring platform.