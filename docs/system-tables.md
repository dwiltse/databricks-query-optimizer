# System Tables for Query Optimization

## Overview
This document outlines the key Databricks system tables and views for building a query optimization platform, based on proven implementations from Databricks Labs Cost Observability project.

## Core System Tables

### Billing & Usage Tables
- `system.billing.usage` - Primary usage tracking
- `system.billing.list_prices` - Price calculation foundation

### Compute Performance Tables
- `system.compute.clusters` - Cluster configuration and performance
- `system.compute.warehouses` - SQL warehouse metrics
- `system.lakeflow.jobs` - Job execution patterns
- `system.lakeflow.pipelines` - Pipeline performance data

### Query Performance Tables (Recommended for Genie Space)
Based on standard Databricks monitoring patterns:
- `system.query.history` - Historical query execution data
- `system.compute.cluster_events` - Cluster lifecycle events
- `system.storage.table_lineage` - Data lineage for optimization
- `system.access.audit` - Access patterns and frequency

## Key Metrics for Query Optimization

### Performance Metrics
- Query execution time
- Resource utilization (CPU, memory, disk)
- Data scan volume
- Shuffle operations
- Cache hit ratios

### Cost Metrics
- DBU consumption per query
- Storage costs associated with queries
- Compute costs by workload type
- Cost per query pattern

### Efficiency Metrics
- Queries per hour/day
- Average query duration
- Resource waste indicators
- Optimization opportunity scores

## Data Model Structure

### Query Performance View
```sql
-- Recommended view for Genie space
CREATE OR REPLACE VIEW query_performance_metrics AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    query_text,
    start_time,
    end_time,
    duration_ms,
    rows_read,
    bytes_read,
    compute_cost_dbu,
    execution_status,
    cluster_id,
    warehouse_id
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 30 DAYS
```

### Cost Attribution View
```sql
-- Based on cost observability patterns
CREATE OR REPLACE VIEW query_cost_attribution AS
SELECT 
    qh.query_id,
    qh.workspace_id,
    qh.duration_ms,
    bu.usage_quantity,
    lp.pricing.default * bu.usage_quantity as estimated_cost,
    qh.cluster_id,
    qh.warehouse_id
FROM system.query.history qh
JOIN system.billing.usage bu ON qh.workspace_id = bu.workspace_id
JOIN system.billing.list_prices lp ON bu.sku_name = lp.sku_name
```

## Optimization Opportunity Identification

### Long-Running Queries
- Queries exceeding 95th percentile duration
- Queries with high resource consumption
- Queries with frequent failures

### Resource Inefficiency
- Queries with low cache utilization
- Queries with excessive data scanning
- Queries causing cluster scaling events

### Cost Optimization
- Queries with high DBU consumption
- Queries suitable for serverless migration
- Queries with optimization potential

## Implementation Notes

### Genie Space Requirements
- Aggregate query metrics by user, workspace, and time period
- Provide drill-down capabilities for detailed analysis
- Support filtering by query patterns and performance thresholds
- Enable forecasting based on historical trends

### MCP Integration Points
- Real-time query monitoring
- Automated recommendation generation
- Performance baseline establishment
- Cost-benefit analysis of optimizations

## TODO: Implementation Steps
- [ ] Create materialized views for performance metrics
- [ ] Implement query classification logic
- [ ] Build optimization recommendation engine
- [ ] Create alerting for performance degradation
- [ ] Develop cost prediction models