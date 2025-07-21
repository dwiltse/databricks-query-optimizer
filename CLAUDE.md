# Databricks Query Optimizer - Claude Code Memory

## Project Overview
This project implements a comprehensive query performance optimization platform for Databricks using system tables, Delta Lake, and Genie Spaces for AI-powered analytics.

## Genie Space Architecture

### Genie Space 1: Real-Time Query Monitoring
**Purpose**: Current performance monitoring and alerts with your established business logic
**Tables** (5 objects following best practices): 
- `system.query.history` (direct system table)
- `system.compute.clusters` (direct system table)
- `mcp.query_optimization.mv_query_performance_categorized` (materialized view with your SLOW/MODERATE/FAST logic)
- `mcp.query_optimization.mv_current_slow_queries` (materialized view for immediate issues)
- `mcp.query_optimization.mv_resource_utilization_alerts` (materialized view for resource monitoring)
**Documentation**: `src/genie-space/genie-space-1-real-time-query-monitoring/instructions.md`

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
**Tables** (4 objects following best practices):
- `mcp.query_optimization.mv_daily_performance` (materialized view for trend analysis)
- `mcp.query_optimization.mv_hourly_performance` (materialized view for detailed patterns)
- `mcp.query_optimization.performance_baselines` (table for baseline management)
- `mcp.query_optimization.query_performance_raw` (table for detailed records)
**Documentation**: `src/genie-space/genie-space-3-performance-analytics/instructions.md`

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

## Databricks Genie Space Best Practices
**References**: 
- [Official Genie Best Practices](http://docs.databricks.com/aws/en/genie/best-practices)
- [Medium: Best Practices for AI/BI Genie Spaces](https://medium.com/dbsql-sme-engineering/best-practices-for-ai-bi-genie-spaces-on-databricks-6f101612c792)

### Data Selection & Modeling
- **Start Small**: 5-10 tables with <50 columns for specific topics
- **Coherent Dataset**: Tables should reflect real-world connections and logical associations
- **Primary/Foreign Keys**: Define relationships so Genie understands table connections
- **Gold Layer Focus**: Use curated business-level tables (Kimball star schema preferred)
- **Remove Excess**: Create views to eliminate columns that don't fit the topic
- **User Permissions**: Ensure all users have SELECT privileges on space data

### Documentation & Annotation
- **Rich Metadata**: Well-annotated tables with clear column names and descriptions
- **Business Context**: Document business logic, company-specific jargon, domain concepts
- **AI-Generated Docs**: Use AI documentation generation while maintaining control
- **Column Descriptions**: Add synonyms and custom descriptions for better matching

### Instructions Design
- **Business Language Transfer**: Define company-specific terms and logic
- **Operational Details**: Fiscal year start dates, filter logic, case sensitivity rules
- **One-Shot Learning**: Provide format examples (e.g., "Firstname Middlename, Lastname")
- **Clear & Concise**: Simple, coherent instructions yield better results

### Query Enhancement Strategies
- **Trusted Assets**: Predefined queries for specific questions (single source of truth)
- **SQL Examples**: Guide Genie on how to query data without executing
- **Serverless Warehouses**: Use for faster startup and intelligent workload management
- **Function Registration**: Create parameterized functions for complex calculations

### Monitoring & Improvement
- **Feedback Loop**: Rate responses with 👍/👎 for continuous improvement
- **Message Feed**: Monitor all questions/answers filtered by time, rating, user, status
- **Iteration**: Use insights to improve instructions and add trusted assets
- **Testing**: Rephrase sample questions and refine until expected responses achieved

### Use Case Strategy
- **Dashboard Offloading**: Use Genie for deep-dive questions beyond standard KPIs
- **10/90 Rule**: Dashboards for top 10% recurring questions, Genie for complex exploration
- **Business User Empowerment**: Enable self-service analytics while reducing data team workload

## MCP Integration Architecture
**Goal**: Enable natural language query analysis and optimization suggestions integrated with Genie Spaces

### MCP-Genie Integration Flow
1. **Query Identification**: System tables identify poor performing/expensive queries
2. **Genie Space Analysis**: Query data flows through optimized Genie spaces for pattern recognition
3. **MCP Chat Interface**: Users chat with specific queries using Genie's natural language capabilities
4. **AI Analysis**: MCP analyzes query patterns, execution plans, and performance metrics via Genie
5. **Trusted Assets**: Pre-built optimization functions provide consistent recommendations
6. **Workspace Integration**: Suggestions pushed back to development workspace for testing

### Genie Space Design for Query Optimization
**Following Best Practices for MCP Integration**:
- **5-Table Limit**: Each optimization space focused on specific query analysis domain
- **Rich Annotations**: Detailed metadata on query patterns, costs, and performance metrics
- **Trusted Assets**: Pre-defined optimization functions for common patterns
- **SQL Examples**: Guide Genie on how to analyze query performance data
- **Business Instructions**: Define optimization terminology and thresholds

### Required System Tables for Query Identification
- `system.query.history` - Query execution details and performance metrics
- `system.billing.usage` - Cost analysis per query  
- `system.compute.clusters` - Resource utilization data
- `system.compute.warehouses` - SQL warehouse performance
- `mcp.query_optimization.query_performance_analysis` - Curated analysis view

### Custom Views for Genie Spaces
- **Expensive Queries View**: Top DBU cost queries with business context
- **Slow Queries View**: Execution time analysis with historical baselines
- **Failed Queries View**: Error patterns with categorization
- **Resource Usage View**: CPU/memory utilization with optimization opportunities
- **Pattern Analysis View**: Query pattern classification with suggested improvements

### MCP Query Chat Features (via Genie)
- **Natural Language Interface**: "Show me why this query is expensive"
- **Contextual Analysis**: Full query text, execution plan, performance metrics
- **Historical Comparison**: "Compare this query to last month's performance"
- **Optimization Suggestions**: "What specific changes would improve this query?"
- **Cost Impact Analysis**: "How much DBU savings would these changes provide?"
- **Testing Integration**: Generate test cases for optimization validation

### Trusted Assets for Query Optimization
- **Cost Analysis Function**: Standardized DBU cost calculation
- **Performance Baseline Function**: Compare against historical performance
- **Optimization Impact Function**: Predict improvement from changes
- **Resource Efficiency Function**: Calculate resource utilization metrics
- **Pattern Classification Function**: Categorize query types for targeted optimization

## Project Updates Required

### High Priority Updates
1. **Genie Space Optimization**: Apply best practices to existing 3 spaces
   - Add rich metadata and column descriptions
   - Create trusted assets for common optimization queries
   - Write comprehensive instructions.md with business context
   - Define primary/foreign key relationships

2. **MCP-Genie Integration Layer**: 
   - Build natural language query analysis via Genie spaces
   - Create trusted optimization functions
   - Implement query chat interface using Genie's capabilities
   - Design feedback loop for continuous improvement

3. **System Table Views Enhancement**:
   - Create 5 focused views per Genie space (following best practices)
   - Add business context and annotations
   - Implement query pattern classification
   - Build cost analysis and performance baselines

4. **Workspace Integration**:
   - Code push-back mechanism for testing optimizations
   - Integration with development workflows
   - Automated testing of optimization suggestions

### Medium Priority Updates
1. **Monitoring & Feedback System**:
   - Implement Genie response rating system
   - Message feed analysis for improvement insights
   - Usage pattern monitoring
   - Iterative space refinement

2. **Performance Optimization**:
   - Serverless SQL warehouse configuration
   - Query execution optimization
   - Resource utilization tracking
   - Impact measurement framework

3. **User Experience**:
   - Natural language query interface
   - Dashboard integration (10/90 rule: dashboards for KPIs, Genie for exploration)
   - Business user self-service capabilities

### Architecture Alignment with Best Practices
- ✅ Current 3 Genie Spaces align with 5-table limit
- ❌ Need comprehensive metadata and annotations
- ❌ Missing trusted assets and SQL examples
- ❌ No primary/foreign key relationships defined
- ❌ Instructions.md files need business context
- ❌ No feedback/monitoring system implemented

### Success Metrics
- **Genie Response Quality**: >80% positive ratings on optimization suggestions
- **User Adoption**: Business users self-serving 70% of optimization questions
- **Query Performance**: 30% reduction in average query execution time
- **Cost Optimization**: 25% reduction in DBU consumption
- **Development Efficiency**: 50% reduction in data team optimization requests

## Next Steps for Additional Genie Spaces
When creating new Genie Spaces:
1. Create folder: `src/genie-space/genie-space-N-[purpose]/`
2. Add `instructions.md` with table relationships and example queries
3. Update this CLAUDE.md file with new space details
4. Follow 5-table limit and focused purpose pattern

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.

# MCP Query Optimization Integration Plan
This project combines Databricks system tables → Genie spaces → MCP integration to create an AI-powered query optimization platform that enables natural language interaction with query performance data and provides actionable optimization suggestions integrated back into development workflows.