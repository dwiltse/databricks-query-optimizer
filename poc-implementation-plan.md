# Databricks Query Optimization POC - Implementation Plan

## Overview
Build a POC that combines Databricks App (Streamlit) with custom MCP server to enable natural language chat with query performance data and AI-powered optimization suggestions.

## Architecture Components

```
[Databricks System Tables] → [Genie Spaces] → [Streamlit Dashboard] ↔ [Custom MCP Server] ↔ [Claude LLM]
                                                      ↓
                                              [Optimized Query Download]
```

## Implementation Steps

### Phase 1: Databricks Environment Setup

#### Step 1.1: Create Genie Spaces Infrastructure
```bash
# In Databricks SQL Editor or Notebook
```

1. **Execute Schema Setup**:
   - Run `src/genie-space/sql/01_schema_setup.sql`
   - Creates `mcp.query_optimization` schema

2. **Create Core Tables**:
   - Run `src/genie-space/sql/02_core_tables.sql`
   - Creates performance tracking tables

3. **Create Materialized Views**:
   - Run `src/genie-space/sql/03_materialized_views.sql`
   - Includes your performance categorization logic

4. **Deploy Trusted Assets**:
   - Run `src/genie-space/trusted-assets/performance_categorization_function.sql`
   - Run `src/genie-space/trusted-assets/cost_impact_analysis_function.sql`

5. **Populate Initial Data**:
   - Run ETL pipeline: `src/genie-space/sql/05_etl_pipeline.sql`
   - Wait 15-30 minutes for materialized views to populate

#### Step 1.2: Create Genie Spaces
1. **Navigate to Databricks SQL** → **AI/BI** → **Genie Spaces**

2. **Create Space 1: Real-Time Query Monitoring**:
   - Name: "Real-Time Query Performance"  
   - Add tables:
     - `system.query.history`
     - `system.compute.clusters`
     - `mcp.query_optimization.mv_query_performance_categorized`
     - `mcp.query_optimization.mv_current_slow_queries`
     - `mcp.query_optimization.mv_resource_utilization_alerts`
   - Upload instructions: `src/genie-space/genie-space-1-real-time-query-monitoring/instructions.md`

3. **Create Space 2: Query Optimization Opportunities**:
   - Name: "Query Optimization Analysis"
   - Add tables from existing instructions
   - Test with questions like "Show me the most expensive queries"

#### Step 1.3: Test Genie Spaces
```sql
-- Test queries in each Genie Space:
-- "Show me slow queries from the last hour"
-- "Which queries have the worst performance scores?"
-- "What are my top optimization opportunities?"
```

### Phase 2: Custom MCP Server Development

#### Step 2.1: MCP Server Setup
```bash
# Create MCP server project
mkdir databricks-query-mcp-server
cd databricks-query-mcp-server

# Initialize Python project
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install mcp databricks-sdk python-dotenv
```

#### Step 2.2: MCP Server Implementation
**File**: `databricks_query_mcp/server.py`
```python
import os
from mcp.server import Server
from mcp.types import Tool, TextContent
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
import json

class DatabricksQueryMCPServer:
    def __init__(self):
        self.workspace_client = WorkspaceClient()
        self.server = Server("databricks-query-optimizer")
        self.setup_tools()
    
    def setup_tools(self):
        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            return [
                Tool(
                    name="analyze_slow_queries",
                    description="Analyze slow queries from Genie Space and provide optimization suggestions",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "time_window_hours": {"type": "integer", "default": 24},
                            "performance_category": {"type": "string", "enum": ["SLOW", "MODERATE", "ALL"]}
                        }
                    }
                ),
                Tool(
                    name="get_query_details",
                    description="Get detailed performance information for a specific query",
                    inputSchema={
                        "type": "object", 
                        "properties": {
                            "query_id": {"type": "string"},
                            "include_optimization_suggestions": {"type": "boolean", "default": True}
                        }
                    }
                ),
                Tool(
                    name="optimize_query_sql",
                    description="Analyze SQL query text and provide optimization recommendations",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "sql_query": {"type": "string"},
                            "performance_metrics": {"type": "object", "optional": True}
                        }
                    }
                )
            ]
    
    async def analyze_slow_queries(self, time_window_hours: int = 24, performance_category: str = "SLOW"):
        # Query Genie Space via SQL warehouse
        query = f"""
        SELECT 
            statement_id,
            statement_text,
            executed_by,
            performance_category,
            execution_duration_ms,
            bytes_per_row_efficiency,
            optimization_flag
        FROM mcp.query_optimization.mv_query_performance_categorized
        WHERE end_time >= CURRENT_TIMESTAMP - INTERVAL {time_window_hours} HOUR
            AND performance_category = '{performance_category}'
        ORDER BY execution_duration_ms DESC
        LIMIT 10
        """
        
        # Execute via SQL warehouse
        result = await self.execute_warehouse_query(query)
        return self.format_query_analysis(result)
    
    async def execute_warehouse_query(self, query: str):
        # Implementation using Databricks SQL warehouse
        # Returns query results for analysis
        pass
    
    def format_query_analysis(self, query_results):
        # Format results with optimization suggestions
        pass

# Server configuration
server = DatabricksQueryMCPServer()
```

#### Step 2.3: Environment Configuration
**File**: `.env`
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_WAREHOUSE_ID=your-sql-warehouse-id
```

### Phase 3: Databricks App Development

#### Step 3.1: Streamlit App Setup
**File**: `databricks_app/query_optimizer_app.py`
```python
import streamlit as st
from databricks import sql
import pandas as pd
import requests
import json

# Following cookbook patterns
@st.cache_resource
def init_connection():
    return sql.connect(
        server_hostname=st.secrets["databricks"]["hostname"],
        http_path=st.secrets["databricks"]["http_path"],
        access_token=st.secrets["databricks"]["access_token"]
    )

def load_query_performance_data():
    conn = init_connection()
    
    query = """
    SELECT 
        statement_id,
        LEFT(statement_text, 100) as query_preview,
        executed_by,
        performance_category,
        execution_duration_ms,
        bytes_per_row_efficiency,
        optimization_flag,
        end_time
    FROM mcp.query_optimization.mv_query_performance_categorized
    WHERE end_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
    ORDER BY execution_duration_ms DESC
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

# Main app
st.title("🚀 Databricks Query Optimizer")
st.subheader("AI-Powered Query Performance Analysis")

# Load performance data
df = load_query_performance_data()

# Display performance dashboard
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Slow Queries", len(df[df['performance_category'] == 'SLOW']))
with col2:
    st.metric("Total Queries", len(df))
with col3:
    st.metric("Avg Duration", f"{df['execution_duration_ms'].mean()/1000:.1f}s")

# Query selection interface
selected_query = st.selectbox(
    "Select a query to analyze:",
    options=df.to_dict('records'),
    format_func=lambda x: f"{x['executed_by']} - {x['query_preview']} ({x['performance_category']})"
)

if selected_query:
    st.write("### Query Details")
    st.code(selected_query['statement_text'] if 'statement_text' in selected_query else 'Query text not available', language='sql')
    
    # Chat interface for optimization
    if st.button("🤖 Analyze with AI"):
        st.write("### AI Analysis & Optimization Suggestions")
        
        # Call MCP server
        mcp_response = call_mcp_server({
            "tool": "get_query_details",
            "query_id": selected_query['statement_id'],
            "include_optimization_suggestions": True
        })
        
        st.write(mcp_response['analysis'])
        
        if 'optimized_query' in mcp_response:
            st.write("### Optimized Query")
            st.code(mcp_response['optimized_query'], language='sql')
            
            # Download button
            st.download_button(
                label="📥 Download Optimized Query",
                data=mcp_response['optimized_query'],
                file_name=f"optimized_query_{selected_query['statement_id']}.sql",
                mime="text/plain"
            )

def call_mcp_server(payload):
    # Implementation to call MCP server
    # Returns AI analysis and suggestions
    pass
```

#### Step 3.2: App Configuration
**File**: `.streamlit/secrets.toml`
```toml
[databricks]
hostname = "your-workspace.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/your-warehouse-id"
access_token = "your-token"

[mcp]
server_url = "http://localhost:8000"
```

### Phase 4: Integration & Testing

#### Step 4.1: End-to-End Testing
1. **Start MCP Server**:
   ```bash
   cd databricks-query-mcp-server
   python -m databricks_query_mcp.server
   ```

2. **Launch Databricks App**:
   ```bash
   cd databricks_app
   streamlit run query_optimizer_app.py
   ```

3. **Test Workflow**:
   - Browse slow queries in dashboard
   - Select query for analysis  
   - Click "Analyze with AI"
   - Review suggestions
   - Download optimized query
   - Test in Databricks workspace

#### Step 4.2: MCP Client Integration
**Alternative**: Use MCP client directly in Streamlit
```python
import mcp
from mcp.client import Client

async def analyze_with_mcp(query_details):
    async with Client("databricks-query-optimizer") as client:
        result = await client.call_tool(
            "optimize_query_sql",
            sql_query=query_details['statement_text'],
            performance_metrics=query_details
        )
        return result
```

## Testing Scenarios

### Scenario 1: Slow Query Optimization
1. **Find slow query** in dashboard (>300s execution time)
2. **Analyze with AI**: Get suggestions for indexes, partitioning, query rewriting
3. **Download optimized query**: Test in Databricks SQL Editor
4. **Compare performance**: Re-run and measure improvement

### Scenario 2: Cost Optimization
1. **Identify expensive queries** by DBU consumption
2. **AI analysis**: Suggestions for reducing data scanning, compute usage
3. **Optimization recommendations**: Materialized views, caching strategies
4. **ROI calculation**: Estimated cost savings

### Scenario 3: Pattern Analysis  
1. **Bulk analysis**: "Show me all SELECT * queries"
2. **Pattern-specific suggestions**: Replace with column-specific selects
3. **Batch optimization**: Generate multiple optimized queries
4. **Implementation tracking**: Monitor adoption of suggestions

## Success Metrics

- **Performance Improvement**: >30% reduction in query execution time
- **Cost Reduction**: >25% reduction in DBU consumption  
- **User Adoption**: Data teams using suggestions 70% of the time
- **Accuracy**: AI suggestions work without modification 80% of the time

## Next Steps After POC

1. **Scale Genie Spaces**: Add more workspaces and query patterns
2. **Enhanced MCP Tools**: Add cost forecasting, impact simulation
3. **Workflow Integration**: Direct integration with Databricks notebooks/repos
4. **Advanced Analytics**: ML-powered performance prediction
5. **Enterprise Features**: Multi-user access, audit logging, approval workflows

## Deployment Considerations

- **Security**: Use service principals for production authentication
- **Scalability**: Consider caching for MCP responses
- **Monitoring**: Track MCP server performance and accuracy
- **Governance**: Implement approval process for auto-generated optimizations

This POC demonstrates the core concept of combining Genie Spaces with MCP for intelligent query optimization with a clear path to production deployment.