# Databricks Query Optimizer - Claude Code Memory

## Project Overview
This project implements a comprehensive query performance optimization platform for Databricks using system tables, Delta Lake, and Genie Spaces for AI-powered analytics.

## Genie Space Architecture

### Genie Space 1: Real-Time Query Monitoring
**Purpose**: Current performance monitoring and alerts
**Tables**: 
- `system.query.history` (direct)
- `system.compute.clusters` (direct)
- `mcp.query_optimization.v_current_slow_queries`
- `mcp.query_optimization.v_current_expensive_queries` 
- `mcp.query_optimization.v_current_failed_queries`

### Genie Space 2: Query Optimization Opportunities
**Purpose**: Identify and track optimization opportunities with ROI analysis
**Tables**:
- `mcp.query_optimization.query_patterns`
- `mcp.query_optimization.optimization_tracking`
- `mcp.query_optimization.mv_pattern_performance`
- `mcp.query_optimization.mv_user_performance`
- `system.billing.usage`
**Documentation**: `src/genie-space/genie-space-2-optimization-opportunities/instructions.md`

### Genie Space 3: Performance Analytics
**Purpose**: Historical performance trending and baseline analysis
**Tables**:
- `mcp.query_optimization.mv_daily_performance`
- `mcp.query_optimization.mv_hourly_performance`
- `mcp.query_optimization.performance_baselines`
- `mcp.query_optimization.query_performance_raw`

## Key Configuration Files
- **Main Config**: `src/genie-space/config.yaml` - All business thresholds and system settings
- **Orchestration**: `src/genie-space/orchestration/query_optimization_orchestration.py` - Production ETL pipeline
- **Validation**: `src/genie-space/validation/data_quality_tests.sql` - Comprehensive data quality tests

## Schema Structure
- **Catalog**: `mcp`
- **Query Optimization Schema**: `mcp.query_optimization`
- **Cost Optimization Schema**: `mcp.cost_optimization` (existing, separate)

## Development Guidelines
1. Each Genie Space should have ≤5 tables (Databricks best practice)
2. Include `instructions.md` for each Genie Space with table relationships and example queries
3. Use hybrid approach: system tables for real-time, created tables for analytics
4. All business rules configured in `config.yaml`
5. Track implementation with `optimization_tracking` table

## Next Steps for Additional Genie Spaces
When creating new Genie Spaces:
1. Create folder: `src/genie-space/genie-space-N-[purpose]/`
2. Add `instructions.md` with table relationships and example queries
3. Update this CLAUDE.md file with new space details
4. Follow 5-table limit and focused purpose pattern