# Application Architecture

## Overview
The Databricks Query Optimizer Platform follows a modular architecture designed around proven dashboard patterns from Databricks Labs Cost Observability and Account Usage Dashboard implementations.

## Architecture Components

### 1. Data Layer
**System Tables Integration**
- Direct connection to Databricks system tables
- Real-time data ingestion from:
  - `system.billing.usage`
  - `system.billing.list_prices`
  - `system.compute.clusters`
  - `system.compute.warehouses`
  - `system.query.history`
  - `system.lakeflow.jobs`

**Data Processing Pipeline**
```
System Tables → Data Validation → Aggregation → Genie Space → MCP Integration
```

### 2. Genie Space Layer
**Purpose**: Centralized analytics workspace for query performance data

**Key Views and Tables**:
```sql
-- Core performance metrics aggregation
CREATE OR REPLACE VIEW genie_query_performance AS
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
WHERE start_time >= current_date() - INTERVAL 90 DAYS;

-- Cost attribution with pricing
CREATE OR REPLACE VIEW genie_cost_attribution AS
SELECT 
    qh.query_id,
    qh.workspace_id,
    qh.user_id,
    qh.duration_ms,
    qh.compute_cost_dbu,
    bu.usage_quantity,
    lp.pricing.default * bu.usage_quantity as estimated_cost,
    DATE(qh.start_time) as cost_date
FROM system.query.history qh
JOIN system.billing.usage bu ON qh.workspace_id = bu.workspace_id
JOIN system.billing.list_prices lp ON bu.sku_name = lp.sku_name
WHERE qh.start_time >= current_date() - INTERVAL 90 DAYS;

-- Optimization opportunities
CREATE OR REPLACE VIEW genie_optimization_opportunities AS
SELECT 
    query_id,
    workspace_id,
    user_id,
    query_text,
    duration_ms,
    bytes_read,
    compute_cost_dbu,
    CASE 
        WHEN UPPER(query_text) LIKE '%SELECT *%' THEN 'SELECT_ALL_OPTIMIZATION'
        WHEN UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%' THEN 'UNBOUNDED_SORT'
        WHEN duration_ms > 300000 THEN 'LONG_RUNNING_QUERY'
        WHEN compute_cost_dbu > 10 THEN 'HIGH_COST_QUERY'
        ELSE 'GENERAL_OPTIMIZATION'
    END as optimization_type,
    CASE 
        WHEN duration_ms > 300000 THEN 'High'
        WHEN compute_cost_dbu > 5 THEN 'Medium'
        ELSE 'Low'
    END as priority
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 30 DAYS
    AND (UPPER(query_text) LIKE '%SELECT *%' 
         OR (UPPER(query_text) LIKE '%ORDER BY%' AND UPPER(query_text) NOT LIKE '%LIMIT%')
         OR duration_ms > 300000
         OR compute_cost_dbu > 10);
```

### 3. MCP Integration Layer
**Real-time Monitoring**
- Stream processing for live query performance tracking
- Automated alert generation based on performance thresholds
- Integration with notification systems

**Recommendation Engine**
- Pattern-based query analysis
- ML-powered optimization suggestions
- Cost-benefit analysis for recommendations

### 4. Customer Application Layer

#### Frontend Architecture
**Dashboard Framework**: Based on proven Databricks Labs patterns
- Modular widget system
- Configurable layouts per user role
- Real-time data refresh capabilities

**Core Dashboard Components**:
1. **Performance Overview**
   - Query volume trends
   - Average execution time
   - Success rate metrics
   - Resource utilization

2. **Cost Analysis**
   - DBU consumption tracking
   - Cost attribution by workspace/user
   - Forecasting and budgeting

3. **Optimization Recommendations**
   - Prioritized improvement opportunities
   - Implementation guidance
   - Impact estimation

4. **Query Deep Dive**
   - Individual query analysis
   - Historical performance trends
   - Resource usage breakdown

#### Backend Services
**API Gateway**
```python
# Query Performance API
@app.route('/api/query-performance/<query_id>')
def get_query_performance(query_id):
    return {
        'query_id': query_id,
        'metrics': get_performance_metrics(query_id),
        'recommendations': get_optimization_recommendations(query_id),
        'cost_analysis': get_cost_breakdown(query_id)
    }

# Dashboard Data API
@app.route('/api/dashboard/performance-overview')
def get_performance_overview():
    return {
        'total_queries': get_total_queries(),
        'avg_duration': get_average_duration(),
        'top_slow_queries': get_top_slow_queries(),
        'cost_trends': get_cost_trends()
    }
```

## Data Flow Architecture

### 1. Ingestion Pipeline
```
System Tables → Data Validation → Transformation → Genie Space Storage
```

### 2. Analytics Pipeline
```
Genie Space → Aggregation → Metric Calculation → Cache → API Layer
```

### 3. Real-time Monitoring
```
System Tables → Stream Processing → Alert Engine → Notification Service
```

## Technology Stack

### Data Processing
- **SQL**: Primary query language for data transformation
- **Python**: Data processing and API development
- **Apache Spark**: Large-scale data processing
- **Delta Lake**: Data storage and versioning

### Application Layer
- **React/TypeScript**: Frontend dashboard
- **Python Flask/FastAPI**: Backend API services
- **Redis**: Caching and session management
- **PostgreSQL**: Application metadata storage

### Monitoring & Alerting
- **Prometheus**: Metrics collection
- **Grafana**: Monitoring dashboards
- **PagerDuty**: Alert routing and escalation

## Security Architecture

### Data Access Control
- Role-based access control (RBAC)
- Workspace-level data isolation
- Query result filtering by user permissions

### API Security
- JWT-based authentication
- Rate limiting and throttling
- Input validation and sanitization

## Scalability Considerations

### Data Volume
- Partitioned tables by date and workspace
- Automated data retention policies
- Incremental processing for large datasets

### Query Performance
- Materialized views for common aggregations
- Query result caching
- Connection pooling for database access

### Application Scaling
- Horizontal scaling for API services
- Load balancing for high availability
- Container orchestration with Kubernetes

## Deployment Architecture

### Development Environment
```yaml
# docker-compose.yml
version: '3.8'
services:
  app:
    build: ./src/databricks-app
    ports:
      - "3000:3000"
    environment:
      - DATABRICKS_HOST=${DATABRICKS_HOST}
      - DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
  
  api:
    build: ./src/mcp-integration
    ports:
      - "8000:8000"
    depends_on:
      - redis
      - postgres
  
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
  
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_DB=query_optimizer
      - POSTGRES_USER=app
      - POSTGRES_PASSWORD=password
```

### Production Environment
- **Cloud Provider**: AWS/Azure/GCP
- **Container Orchestration**: Kubernetes
- **Database**: Databricks SQL Warehouse
- **Monitoring**: Integrated with Databricks system monitoring

## Integration Points

### Databricks Integration
- Unity Catalog for data governance
- Databricks SQL for query execution
- Delta Sharing for data collaboration

### External Systems
- LDAP/Active Directory for authentication
- Slack/Teams for notifications
- JIRA for issue tracking

## Performance Metrics

### Application Performance
- API response time < 500ms
- Dashboard load time < 3 seconds
- Query execution time monitoring

### Data Processing
- Batch processing latency < 15 minutes
- Real-time stream processing < 5 seconds
- Data freshness monitoring

## TODO: Implementation Steps
- [ ] Set up Genie space with system table connections
- [ ] Implement core data transformation views
- [ ] Build REST API for dashboard data
- [ ] Create React-based dashboard frontend
- [ ] Implement real-time alerting system
- [ ] Add user authentication and authorization
- [ ] Create deployment pipelines
- [ ] Implement monitoring and logging
- [ ] Add automated testing framework
- [ ] Create documentation and user guides