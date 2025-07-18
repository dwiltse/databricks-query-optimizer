# POC Genie Space Plan: Query Optimization Monitoring

## Essential System Tables (3 core tables)

**1. `system.query.history`**
- Primary table containing all query execution data
- Key fields: `query_id`, `query_text`, `user_name`, `execution_time_ms`, `total_task_duration_ms`, `executed_by`, `start_time`, `end_time`, `warehouse_id`, `compute.warehouse_name`
- Filter recommendation: `start_time >= current_date() - 30` for POC

**2. `system.compute.warehouses`** 
- Warehouse configuration and sizing information
- Key fields: `id`, `name`, `cluster_size`, `min_num_clusters`, `max_num_clusters`, `auto_stop_mins`
- Join: `system.query.history.warehouse_id = system.compute.warehouses.id`

**3. `system.billing.usage`**
- Cost information for queries and compute
- Key fields: `account_id`, `workspace_id`, `sku_name`, `usage_date`, `usage_unit`, `usage_quantity`, `usage_metadata`
- Filter: `usage_date >= current_date() - 30` and `sku_name LIKE '%SQL%'`

## Table Relationships & Join Patterns

```sql
-- Primary join pattern for query performance analysis
SELECT 
    qh.query_id,
    qh.execution_time_ms,
    qh.user_name,
    qh.query_text,
    w.name as warehouse_name,
    w.cluster_size,
    qh.start_time
FROM system.query.history qh
LEFT JOIN system.compute.warehouses w ON qh.warehouse_id = w.id
WHERE qh.start_time >= current_date() - 30
```

## Suggested Optimized Table: `query_performance_summary`

**Creates a single denormalized table with essential fields:**

```sql
CREATE OR REPLACE TABLE mcp.query_optimization.query_performance_summary AS
SELECT 
    qh.query_id,
    qh.query_text,
    qh.user_name,
    qh.executed_by,
    qh.start_time,
    qh.end_time,
    qh.execution_time_ms,
    qh.total_task_duration_ms,
    qh.rows_produced_count,
    qh.warehouse_id,
    w.name as warehouse_name,
    w.cluster_size,
    w.min_num_clusters,
    w.max_num_clusters,
    -- Business rule classifications
    CASE 
        WHEN qh.execution_time_ms > 300000 THEN 'slow'
        WHEN qh.execution_time_ms > 60000 THEN 'moderate' 
        ELSE 'fast' 
    END as performance_category,
    -- Cost estimation (simplified)
    ROUND(qh.execution_time_ms / 1000.0 / 3600 * w.cluster_size * 0.40, 2) as estimated_cost_usd
FROM system.query.history qh
LEFT JOIN system.compute.warehouses w ON qh.warehouse_id = w.id
WHERE qh.start_time >= current_date() - 30
  AND qh.execution_time_ms IS NOT NULL
```

## Genie Space Instructions for Efficient Queries

**Include in Genie Space instructions.md:**

```markdown
## Efficient Query Patterns

### For performance analysis, prefer:
- `query_performance_summary` table (pre-joined, filtered to 30 days)
- Always filter by `start_time` when using raw system tables
- Use `performance_category` field instead of calculating execution time thresholds

### Table Relationships:
- `system.query.history.warehouse_id` → `system.compute.warehouses.id`
- `system.billing.usage.usage_metadata.warehouse_id` → `system.compute.warehouses.id`

### Sample Questions:
- "Show me the slowest queries from last week"
- "Which users have the most expensive queries?"
- "What's the average query time by warehouse size?"
```

## Implementation Steps for POC

1. **Create the optimized table** using the SQL above
2. **Set up Genie Space** with these 4 tables:
   - `system.query.history`
   - `system.compute.warehouses`
   - `system.billing.usage`
   - `mcp.query_optimization.query_performance_summary`
3. **Add the instructions** to help Genie understand relationships
4. **Test with sample questions** like those listed above

## Benefits

- Single table reduces join complexity for Genie
- Pre-calculated business rules (slow/fast categories)
- 30-day scope keeps data manageable
- Essential fields only (no unnecessary columns)
- Cost estimation included for immediate ROI analysis