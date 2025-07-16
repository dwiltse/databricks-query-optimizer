# Genie Space Requirements

## Overview
The Genie Space serves as the central analytics workspace for query optimization data, providing aggregated views and insights from Databricks system tables.

## Core Requirements

### 1. Data Ingestion
- **Real-time ingestion** from Databricks system tables
- **Incremental processing** for large datasets
- **Data validation** and quality checks
- **Error handling** and retry mechanisms

### 2. Data Storage
- **Partitioned tables** by date and workspace for performance
- **Retention policies** for historical data (90 days active, 1 year archived)
- **Compression** optimization for cost efficiency
- **Indexing** on frequently queried columns

### 3. Query Performance
- **Sub-second response times** for dashboard queries
- **Materialized views** for common aggregations
- **Query caching** for frequently accessed data
- **Connection pooling** for database efficiency

## Data Model Requirements

### Core Tables

#### Query Performance Table
```sql
CREATE TABLE genie_query_performance (
    query_id STRING,
    workspace_id STRING,
    user_id STRING,
    query_text STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_ms BIGINT,
    rows_read BIGINT,
    bytes_read BIGINT,
    compute_cost_dbu DECIMAL(10,4),
    execution_status STRING,
    cluster_id STRING,
    warehouse_id STRING,
    query_date DATE,
    query_hour INT
) PARTITIONED BY (query_date, workspace_id);
```

#### Cost Attribution Table
```sql
CREATE TABLE genie_cost_attribution (
    query_id STRING,
    workspace_id STRING,
    user_id STRING,
    cost_date DATE,
    compute_cost_dbu DECIMAL(10,4),
    estimated_cost_usd DECIMAL(10,2),
    usage_quantity DECIMAL(10,4),
    sku_name STRING
) PARTITIONED BY (cost_date, workspace_id);
```

#### Optimization Opportunities Table
```sql
CREATE TABLE genie_optimization_opportunities (
    opportunity_id STRING,
    query_id STRING,
    workspace_id STRING,
    user_id STRING,
    optimization_type STRING,
    priority STRING,
    description STRING,
    estimated_savings_dbu DECIMAL(10,4),
    implementation_effort STRING,
    created_at TIMESTAMP,
    status STRING
) PARTITIONED BY (DATE(created_at), workspace_id);
```

### Aggregation Views

#### Daily Performance Summary
```sql
CREATE MATERIALIZED VIEW genie_daily_performance AS
SELECT 
    workspace_id,
    user_id,
    query_date,
    COUNT(*) as total_queries,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    SUM(bytes_read) as total_bytes_read,
    COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) as failed_queries
FROM genie_query_performance
GROUP BY workspace_id, user_id, query_date;
```

#### Hourly Trend Analysis
```sql
CREATE MATERIALIZED VIEW genie_hourly_trends AS
SELECT 
    workspace_id,
    query_date,
    query_hour,
    COUNT(*) as query_count,
    AVG(duration_ms) as avg_duration_ms,
    SUM(compute_cost_dbu) as total_cost_dbu,
    COUNT(DISTINCT user_id) as active_users
FROM genie_query_performance
WHERE query_date >= current_date() - INTERVAL 30 DAYS
GROUP BY workspace_id, query_date, query_hour;
```

## Analytics Requirements

### 1. Performance Metrics
- **Query volume trends** (hourly, daily, weekly)
- **Execution time percentiles** (50th, 95th, 99th)
- **Resource utilization** (CPU, memory, disk)
- **Success rate tracking**
- **Cost per query analysis**

### 2. Optimization Identification
- **Long-running query detection**
- **Resource-intensive pattern analysis**
- **Cost optimization opportunities**
- **Query pattern classification**
- **Anomaly detection**

### 3. User and Workspace Analytics
- **Usage patterns by user**
- **Workspace-level performance**
- **Cost attribution and chargeback**
- **Comparative analysis**
- **Trend forecasting**

## Data Processing Requirements

### 1. ETL Pipeline
```sql
-- Incremental data processing
INSERT INTO genie_query_performance
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
    warehouse_id,
    DATE(start_time) as query_date,
    HOUR(start_time) as query_hour
FROM system.query.history
WHERE start_time >= '${last_processed_timestamp}'
    AND start_time < '${current_timestamp}';
```

### 2. Data Quality Checks
- **Null value validation**
- **Data type consistency**
- **Referential integrity**
- **Duplicate detection**
- **Outlier identification**

### 3. Data Enrichment
- **User metadata addition**
- **Workspace information**
- **Query categorization**
- **Performance benchmarking**
- **Cost calculation**

## API Requirements

### 1. Query Performance API
```python
# Query performance endpoint
GET /api/v1/query-performance/{query_id}
Response: {
    "query_id": "string",
    "workspace_id": "string",
    "user_id": "string",
    "performance_metrics": {
        "duration_ms": "integer",
        "bytes_read": "integer",
        "compute_cost_dbu": "decimal"
    },
    "optimization_opportunities": [
        {
            "type": "string",
            "description": "string",
            "priority": "string",
            "estimated_savings": "decimal"
        }
    ]
}
```

### 2. Dashboard Data API
```python
# Dashboard metrics endpoint
GET /api/v1/dashboard/metrics
Parameters: {
    "workspace_id": "string",
    "start_date": "date",
    "end_date": "date",
    "user_id": "string" (optional)
}
Response: {
    "total_queries": "integer",
    "avg_duration_ms": "integer",
    "total_cost_dbu": "decimal",
    "success_rate": "decimal",
    "trends": [
        {
            "date": "date",
            "query_count": "integer",
            "avg_duration": "integer",
            "total_cost": "decimal"
        }
    ]
}
```

## Security Requirements

### 1. Data Access Control
- **Row-level security** by workspace
- **Column-level masking** for sensitive data
- **Role-based access control**
- **Audit logging** for all access

### 2. API Security
- **Authentication** via JWT tokens
- **Authorization** based on workspace membership
- **Rate limiting** to prevent abuse
- **Input validation** for all parameters

## Performance Requirements

### 1. Query Performance
- **Dashboard queries** < 3 seconds
- **API responses** < 1 second
- **Batch processing** < 15 minutes
- **Real-time alerts** < 30 seconds

### 2. Scalability
- **Support for 1000+ concurrent users**
- **Handle 10M+ queries per day**
- **Horizontal scaling capability**
- **Auto-scaling based on load**

## Monitoring Requirements

### 1. System Monitoring
- **Query execution metrics**
- **Resource utilization tracking**
- **Error rate monitoring**
- **Performance trending**

### 2. Business Metrics
- **Cost optimization impact**
- **User adoption rates**
- **Recommendation effectiveness**
- **Platform ROI measurement**

## TODO: Implementation Steps
- [ ] Set up Genie workspace and permissions
- [ ] Create core tables and views
- [ ] Implement ETL pipeline for data ingestion
- [ ] Build API endpoints for data access
- [ ] Create materialized views for performance
- [ ] Implement data quality monitoring
- [ ] Set up security and access controls
- [ ] Create monitoring and alerting
- [ ] Implement automated testing
- [ ] Deploy to production environment