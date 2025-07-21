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
**Reference**: [Microsoft Azure Databricks System Tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/)

- `system.query.history` - All queries run on SQL warehouses and serverless compute (180 days retention)
- `system.compute.clusters` - Full history of compute configurations (365 days retention)
- `system.compute.node_timeline` - Utilization metrics of compute resources (90 days retention)
- `system.billing.usage` - All billable usage across account (365 days retention)
- `system.compute.cluster_events` - Cluster lifecycle events
- `system.storage.table_lineage` - Data lineage for optimization
- `system.access.audit` - Access patterns and frequency

### Key System Table Characteristics
- **Read-only**: All system tables are read-only
- **Schema Evolution**: New columns may be added, queries should be flexible
- **Streaming**: Requires `skipChangeCommits = true` for streaming workloads
- **Global Perspective**: Billing tables provide account-wide view

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

## System.Query.History Field Definitions
**Reference**: [Query History Table Schema](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/query-history)

### Key Performance & Cost Fields

#### Execution Metrics
- `total_duration_ms` - Total query execution time (BIGINT)
- `waiting_for_compute_duration_ms` - Time waiting for compute provisioning
- `execution_duration_ms` - Actual statement execution time  
- `compilation_duration_ms` - Metadata loading and query optimization time

#### Resource Utilization
- `read_rows` - Total rows read (BIGINT)
- `read_bytes` - Total data read size (BIGINT)
- `read_io_cache_percent` - Percentage of data read from IO cache (DOUBLE)
- `spilled_local_bytes` - Data temporarily written to disk (BIGINT)
- `shuffle_read_bytes` - Network data transfer volume (BIGINT)

#### Compute Context
- `compute` - Warehouse or serverless compute identifier (STRUCT)
- `executed_by_user_id` - User running the query (BIGINT)
- `client_application` - Source of query execution (STRING)

#### Performance Indicators
- `from_result_cache` - Result was cached (BOOLEAN)
- `execution_status` - Query completion state (STRING)
- `query_text` - Full SQL query text (STRING)

### Query Performance View
```sql
-- Enhanced view with detailed performance metrics
CREATE OR REPLACE VIEW query_performance_metrics AS
SELECT 
    query_id,
    workspace_id,
    executed_by_user_id as user_id,
    query_text,
    created_time as start_time,
    end_time,
    total_duration_ms,
    execution_duration_ms,
    waiting_for_compute_duration_ms,
    compilation_duration_ms,
    read_rows,
    read_bytes,
    read_io_cache_percent,
    spilled_local_bytes,
    shuffle_read_bytes,
    from_result_cache,
    execution_status,
    compute.warehouse_id,
    client_application
FROM system.query.history
WHERE created_time >= current_date() - INTERVAL 30 DAYS
    AND execution_status = 'FINISHED'
```

## System.Billing.Usage Field Definitions  
**Reference**: [Billing Usage Table Schema](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)

### Key Cost Analysis Fields

#### Core Billing Metrics
- `usage_quantity` - Number of units consumed (DECIMAL)
- `usage_unit` - Billing measurement unit, typically "DBU" (STRING)
- `usage_start_time` / `usage_end_time` - Usage time window in UTC (TIMESTAMP)

#### Cost Segmentation
- `workspace_id` - Workspace identifier for cost allocation (BIGINT)
- `cloud` - Cloud provider (AWS/Azure/GCP) (STRING)
- `sku_name` - Specific pricing tier/product (STRING)
- `billing_origin_product` - Detailed product breakdown (STRING)

#### Resource Attribution
- `usage_metadata` - Resource-specific details (STRUCT)
  - `cluster_id` - Compute cluster identifier
  - `job_id` - Job execution identifier
  - `warehouse_id` - SQL warehouse identifier
  - `node_type` - Instance type used

#### Advanced Cost Tracking
- `custom_tags` - User-defined cost categorization (MAP)
- `record_type` - Usage corrections support (STRING)
- `product_features` - Detailed tier/serverless information (ARRAY)

### Cost Attribution View
```sql
-- Enhanced cost analysis with detailed billing attribution
CREATE OR REPLACE VIEW query_cost_attribution AS
SELECT 
    qh.query_id,
    qh.workspace_id,
    qh.total_duration_ms,
    qh.execution_duration_ms,
    bu.usage_quantity as dbu_consumed,
    bu.usage_unit,
    bu.usage_start_time,
    bu.usage_end_time,
    bu.sku_name,
    bu.billing_origin_product,
    bu.usage_metadata.cluster_id,
    bu.usage_metadata.warehouse_id,
    bu.usage_metadata.node_type,
    bu.custom_tags,
    lp.pricing.default * bu.usage_quantity as estimated_cost_usd
FROM system.query.history qh
LEFT JOIN system.billing.usage bu 
    ON qh.workspace_id = bu.workspace_id
    AND qh.compute.warehouse_id = bu.usage_metadata.warehouse_id
    AND qh.created_time >= bu.usage_start_time 
    AND qh.end_time <= bu.usage_end_time
LEFT JOIN system.billing.list_prices lp 
    ON bu.sku_name = lp.sku_name
WHERE qh.created_time >= current_date() - INTERVAL 30 DAYS
    AND bu.usage_unit = 'DBU'
```

## System.Compute Field Definitions
**Reference**: [Compute System Tables Schema](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/compute)

### System.Compute.Clusters - Configuration Tracking
#### Cluster Configuration
- `cluster_id` - Unique compute resource identifier (STRING)
- `worker_count` - Number of worker nodes (INT)
- `driver_node_type` - Node type for driver (STRING)
- `worker_node_type` - Node type for workers (STRING)
- `dbr_version` - Databricks Runtime version (STRING)
- `auto_termination_minutes` - Auto-shutdown configuration (INT)

### System.Compute.Node_Timeline - Resource Utilization
#### CPU Metrics (Minute Granularity)
- `cpu_user_percent` - CPU time in userland (DOUBLE)
- `cpu_system_percent` - CPU time in kernel (DOUBLE)
- `cpu_wait_percent` - CPU waiting for I/O (DOUBLE)

#### Memory Utilization
- `mem_used_percent` - Memory utilization percentage (DOUBLE)
- `mem_swap_percent` - Memory used for swapping (DOUBLE)

#### Network Performance
- `network_sent_bytes` - Network traffic sent (BIGINT)
- `network_received_bytes` - Network traffic received (BIGINT)

#### Resource Context
- `node_type` - Cloud instance type identifier (STRING)

### System.Compute.Node_Types - Hardware Specifications
- `core_count` - Number of vCPUs (INT)
- `memory_mb` - Total memory in MB (BIGINT)
- `gpu_count` - Number of GPUs (INT)

### Compute Performance Analysis View
```sql
-- Resource utilization analysis with hardware context
CREATE OR REPLACE VIEW compute_performance_analysis AS
SELECT 
    nt.cluster_id,
    nt.timestamp,
    nt.node_type,
    nt.cpu_user_percent + nt.cpu_system_percent as total_cpu_percent,
    nt.cpu_wait_percent,
    nt.mem_used_percent,
    nt.mem_swap_percent,
    nt.network_sent_bytes + nt.network_received_bytes as total_network_bytes,
    nts.core_count,
    nts.memory_mb,
    nts.gpu_count,
    c.worker_count,
    c.dbr_version
FROM system.compute.node_timeline nt
LEFT JOIN system.compute.node_types nts ON nt.node_type = nts.node_type
LEFT JOIN system.compute.clusters c ON nt.cluster_id = c.cluster_id
WHERE nt.timestamp >= current_date() - INTERVAL 7 DAYS
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