# Databricks Query Optimizer Platform

## Overview
A comprehensive customer-facing Databricks application that helps users identify poorly performing queries and receive actionable optimization recommendations. The platform leverages Databricks system tables, Genie space analytics, and MCP integration to provide real-time insights for query performance improvement.

## Key Features
- **Real-time Query Monitoring**: Track query performance as it happens
- **Cost Optimization**: Identify and reduce expensive queries
- **Automated Recommendations**: AI-powered suggestions for query improvements
- **Interactive Dashboard**: User-friendly interface for performance analysis
- **Historical Trending**: Track performance improvements over time

## Architecture
```
System Tables → Genie Space → MCP Integration → Customer App
```

1. **System Tables**: Extract performance and cost data from Databricks
2. **Genie Space**: Aggregate and analyze metrics for insights
3. **MCP Integration**: Real-time monitoring and recommendation engine
4. **Customer App**: Dashboard and optimization interface

## Project Structure
```
databricks-query-optimizer/
├── claude.md                     # Project overview and reference links
├── README.md                     # This file
├── docs/
│   ├── databricks-context.md     # Databricks platform context
│   ├── system-tables.md          # System table data model
│   ├── genie-requirements.md     # Genie space specifications
│   └── app-architecture.md       # Technical architecture
├── references/
│   ├── api-docs.md              # API documentation
│   └── examples.md              # Sample queries and patterns
└── src/
    ├── genie-space/             # Genie space implementation
    ├── mcp-integration/         # MCP connector and monitoring
    └── databricks-app/          # Customer-facing application
```

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Access to system tables (`system.billing.*`, `system.compute.*`, etc.)
- Python 3.8+
- Node.js 16+

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd databricks-query-optimizer

# Install backend dependencies
pip install -r requirements.txt

# Install frontend dependencies
cd src/databricks-app
npm install
```

### Configuration
1. Set up Databricks authentication
2. Configure system table access
3. Create Genie space workspace
4. Configure MCP integration

## Core Metrics Tracked
- **Performance**: Query duration, resource utilization, success rates
- **Cost**: DBU consumption, cost per query, budget tracking
- **Efficiency**: Cache hit ratios, data scan optimization, parallelization
- **Patterns**: Query types, optimization opportunities, user behavior

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

## Technology Stack
- **Backend**: Python (Flask/FastAPI), SQL
- **Frontend**: React, TypeScript
- **Database**: Databricks SQL, Redis (caching)
- **Processing**: Apache Spark, Delta Lake
- **Monitoring**: Prometheus, Grafana

## Development

### Running Locally
```bash
# Start backend API
cd src/mcp-integration
python app.py

# Start frontend development server
cd src/databricks-app
npm start

# Access the application at http://localhost:3000
```

### Testing
```bash
# Run backend tests
pytest tests/

# Run frontend tests
cd src/databricks-app
npm test
```

## API Usage
The platform provides REST APIs for accessing query performance data:

```bash
# Get query performance details
curl -H "Authorization: Bearer <token>" \
  https://api.databricks-query-optimizer.com/v1/queries/q_123/performance

# Get dashboard metrics
curl -H "Authorization: Bearer <token>" \
  "https://api.databricks-query-optimizer.com/v1/dashboard/performance-overview?start_date=2023-12-01&end_date=2023-12-31"
```

See [API Documentation](references/api-docs.md) for complete endpoint details.

## Key Implementation References
- [Databricks Labs Cost Observability](https://github.com/databrickslabs/sandbox/tree/main/cost-observability)
- [Account Usage Dashboard Template](https://github.com/databrickslabs/sandbox/blob/main/cost-observability/Account%20Usage%20Dashboard%20v2.lvdash.json)
- [System Tables Documentation](docs/system-tables.md)

## Success Metrics
- **Performance**: Average query duration reduction
- **Cost**: Total DBU consumption decrease
- **Adoption**: User engagement with recommendations
- **Impact**: Measurable cost savings achieved

## Contributing
1. Review [App Architecture](docs/app-architecture.md) for technical details
2. Check [Genie Requirements](docs/genie-requirements.md) for data model specs
3. Follow existing code patterns and conventions
4. Add tests for new features
5. Update documentation as needed

## Support
For questions or issues:
- Check the [documentation](docs/) directory
- Review [example queries](references/examples.md)
- Refer to [API documentation](references/api-docs.md)

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Next Steps
- [ ] Set up development environment
- [ ] Configure Databricks workspace access
- [ ] Implement core data pipeline
- [ ] Build initial dashboard prototype
- [ ] Add optimization recommendation engine
- [ ] Create user authentication system
- [ ] Deploy to production environment

## Roadmap
- **Phase 1**: Basic query monitoring and cost tracking
- **Phase 2**: Advanced optimization recommendations
- **Phase 3**: Machine learning-powered insights
- **Phase 4**: Integration with external BI tools
- **Phase 5**: Enterprise features and scaling