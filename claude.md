# Databricks Query Optimizer Platform

## Project Overview
A customer-facing Databricks application that helps users identify poorly performing queries and receive optimization recommendations. The platform uses Databricks system tables → Genie space → MCP integration to provide actionable insights for query performance improvement.

## Architecture Flow
1. **System Tables** → Extract query performance and cost data
2. **Genie Space** → Aggregate and analyze performance metrics
3. **MCP Integration** → Real-time monitoring and recommendations
4. **Customer App** → Dashboard and optimization interface

## Key Features
- Real-time query performance monitoring
- Cost optimization recommendations
- Historical trend analysis
- Automated alerting for performance issues
- Interactive optimization dashboard

## Reference Links

### Proven Examples
- [Capital One Health Check Dashboard](https://www.capitalone.com/software/blog/databricks-health-check-dashboard-queries/) - Query performance monitoring patterns
- [Databricks Labs Cost Observability](https://github.com/databrickslabs/sandbox/tree/main/cost-observability) - System table usage and cost tracking
- [Account Usage Dashboard Template](https://github.com/databrickslabs/sandbox/blob/main/cost-observability/Account%20Usage%20Dashboard%20v2.lvdash.json) - Dashboard design patterns

### Documentation
- [System Tables Reference](./docs/system-tables.md) - Data model and table structure
- [Query Examples](./references/examples.md) - Sample queries and dashboard concepts
- [App Architecture](./docs/app-architecture.md) - Technical implementation details
- [Genie Requirements](./docs/genie-requirements.md) - Genie space specifications

## Project Structure
```
databricks-query-optimizer/
├── claude.md                     # This file - project overview
├── README.md                     # Project documentation
├── docs/
│   ├── databricks-context.md     # Databricks platform context
│   ├── system-tables.md          # System table data model
│   ├── genie-requirements.md     # Genie space requirements
│   └── app-architecture.md       # Technical architecture
├── references/
│   ├── api-docs.md              # API documentation
│   └── examples.md              # Sample queries and patterns
└── src/
    ├── genie-space/             # Genie space implementation
    ├── mcp-integration/         # MCP connector and monitoring
    └── databricks-app/          # Customer-facing application
```

## Core System Tables Used
- `system.billing.usage` - Cost and usage tracking
- `system.billing.list_prices` - Price calculation
- `system.compute.clusters` - Cluster performance
- `system.compute.warehouses` - SQL warehouse metrics
- `system.query.history` - Query execution history
- `system.lakeflow.jobs` - Job performance data

## Key Metrics Tracked
- Query execution duration
- Resource utilization (CPU, memory, disk)
- DBU consumption and cost
- Data scan volume
- Cache hit ratios
- Query success rates

## Optimization Categories
1. **Performance Optimization**
   - Long-running queries
   - Resource-intensive operations
   - Inefficient data access patterns

2. **Cost Optimization**
   - High DBU consumption queries
   - Serverless migration opportunities
   - Resource waste reduction

3. **Query Pattern Optimization**
   - SELECT * usage
   - Missing LIMIT clauses
   - Inefficient JOIN patterns
   - Unpartitioned filters

## Implementation TODO
- [ ] Set up Genie space with system table connections
- [ ] Create query performance aggregation views
- [ ] Implement MCP integration for real-time monitoring
- [ ] Build customer dashboard with proven patterns
- [ ] Create recommendation engine with ML models
- [ ] Implement automated alerting system
- [ ] Add optimization tracking and impact measurement
- [ ] Create cost forecasting capabilities
- [ ] Build user-specific optimization recommendations
- [ ] Implement query pattern classification
- [ ] Add baseline performance establishment
- [ ] Create optimization impact reporting

## Development Notes
- Focus on actionable recommendations customers can implement
- Use proven dashboard patterns from reference examples
- Ensure real-time monitoring capabilities
- Implement modular, configurable dashboard design
- Support drill-down from high-level metrics to detailed analysis
- Include cost-benefit analysis for optimization recommendations

## Success Metrics
- Reduction in average query duration
- Decrease in total DBU consumption
- Improvement in query success rates
- User adoption of optimization recommendations
- Cost savings achieved through platform usage