# Query Optimization Dashboard - Databricks MCP POC

A Streamlit dashboard that connects to Databricks Managed MCP to provide natural language query optimization insights.

## Features

- 🚀 **Natural Language Interface**: Ask questions about query performance in plain English
- 🔒 **Secure Access**: Uses Databricks Managed MCP for secure, authenticated data access
- 📊 **Interactive Visualizations**: Automatic charts and insights from query data
- 🎯 **Optimization Recommendations**: AI-powered suggestions for query improvements
- 💰 **Cost Analysis**: Track potential savings from query optimizations

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Databricks Authentication

**Option A: Using Databricks CLI (Recommended)**
```bash
databricks configure --token
```

**Option B: Environment Variables**
```bash
cp .env.example .env
# Edit .env with your Databricks workspace URL and token
```

### 3. Run the Dashboard
```bash
streamlit run query_optimization_dashboard.py
```

## Usage

### Natural Language Queries
Try asking questions like:
- "What are the top 3 slowest queries this week?"
- "Which users have the most expensive queries?"
- "Show me queries with the highest optimization potential"
- "What are the most common query patterns that need optimization?"

### Pre-built Analytics
The dashboard also provides:
- Executive summary metrics
- Top optimization opportunities
- Cost impact analysis
- One-click fix deployment (simulated)

## Architecture

```
Streamlit Dashboard → Databricks Managed MCP → Genie Space (system_table_mcp_test) → Query Tables
                                             ↓
                         Natural Language AI Response + Structured Data
```

## Genie Space Integration

This dashboard connects to the `system_table_mcp_test` Genie Space, which contains:
- `query_performance_raw` - Historical query performance data
- `query_patterns` - Identified optimization patterns
- `optimization_tracking` - Applied optimizations and their impact
- `performance_baselines` - Performance benchmarks

## Development Notes

- The MCP connection is cached for performance
- Queries are automatically scoped to the specific Genie Space
- Error handling includes helpful debugging information
- Sample data is provided for demo purposes while MCP integration is being refined

## Troubleshooting

**Connection Issues:**
- Verify Databricks CLI is configured: `databricks auth login`
- Check workspace permissions for MCP access
- Ensure the Genie Space `system_table_mcp_test` exists and is accessible

**Query Issues:**
- Questions should be specific and reference time ranges when possible
- The Genie Space needs populated data to return meaningful results
- Try the pre-defined questions first to test the connection