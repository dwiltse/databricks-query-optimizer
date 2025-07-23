# Deploy Query Optimization Agent to Databricks Playground

## Step 1: Prepare the Agent Code

1. **Upload the agent configuration**:
   - Copy `agent_config.py` to your Databricks workspace
   - Put it in a notebook or upload as a Python file

2. **Update configuration**:
   ```python
   # In agent_config.py, verify these settings:
   GENIE_SPACE_ID = "system_table_mcp_test"  # Your actual Genie space ID
   LLM_ENDPOINT = "databricks-claude-3-7-sonnet"  # Your Claude endpoint name
   ```

## Step 2: Test the Agent Connection

Before deploying to playground, test the MCP connection:

```python
# Run this in a Databricks notebook
%run ./agent_config.py test
```

This will:
- ✅ Test connection to your Genie space
- ✅ List available MCP tools  
- ✅ Run sample queries
- ✅ Verify Claude Sonnet integration

## Step 3: Deploy to Agent Framework

### Option A: Via Databricks UI

1. **Go to Agent Framework**:
   - Navigate to your Databricks workspace
   - Go to "Agents" or "AI Playground" section

2. **Create New Agent**:
   - Click "Create Agent"
   - Choose "Custom Agent" or "Import Code"

3. **Configure Agent**:
   - **Name**: `Query Optimization Assistant`
   - **Description**: `AI assistant for Databricks query optimization using system tables`
   - **Code**: Upload or paste `agent_config.py`

4. **Set MCP Configuration**:
   - **MCP Server URL**: `https://your-workspace/api/2.0/mcp/genie/system_table_mcp_test`
   - **Authentication**: Use workspace authentication

### Option B: Via MLflow (Programmatic)

```python
import mlflow
from agent_config import QueryOptimizationAgent

# Log the agent model
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="query_optimization_agent",
        python_model=QueryOptimizationAgent(),
        signature=mlflow.types.schema.Schema([
            mlflow.types.ColSpec("string", "question")
        ])
    )

# Register for serving
model_uri = "runs:/<run_id>/query_optimization_agent"
mlflow.register_model(model_uri, "query_optimization_agent")
```

## Step 4: Test in Playground

Once deployed, test these sample questions:

### 🐌 **Slow Query Analysis**
```
"What are the 5 slowest queries in my system in the last 24 hours? 
Provide specific optimization recommendations for each."
```

### 💰 **Cost Optimization**  
```
"Show me the most expensive queries by DBU consumption. 
Calculate potential savings if I optimize the top 3."
```

### 📊 **Pattern Analysis**
```
"Analyze my query patterns over the last week. 
What materialized views should I create?"
```

### 🎯 **Specific Query Help**
```
"I have a query that takes 5 minutes to run and scans 1TB of data. 
What optimization strategies should I try?"
```

## Step 5: Expected Agent Behavior

The agent will:

1. **🔧 Use Genie Space Tool**: Query your system tables via MCP
2. **🤖 Analyze with Claude**: Apply AI intelligence to the data  
3. **💡 Provide Recommendations**: Give specific, actionable advice
4. **📈 Show Impact**: Estimate time/cost savings when possible

### Sample Interaction Flow:

```
👤 User: "What are my slowest queries?"

🔧 Agent calls Genie space tool: query(question="slowest queries last 24 hours")

📊 Genie returns: System table data with query performance metrics

🤖 Claude analyzes: Performance patterns, bottlenecks, optimization opportunities  

💬 Agent responds: 
"I found 5 queries taking >2 minutes each. The slowest is a JOIN query 
scanning 500GB. Here are specific optimizations:
1. Add index on customer_id (estimated 60% speedup)
2. Use materialized view for product_catalog lookup (estimated 40% speedup)
3. Consider partitioning orders table by date (estimated 30% speedup)
..."
```

## Step 6: Create Secure App (Next Phase)

After playground testing works, we'll build the secure Databricks App:

```python
# Future: Secure Databricks App
# - Uses same MCP connection
# - Adds user authentication  
# - Implements role-based access
# - Provides web interface
# - Keeps all data within Databricks
```

## Troubleshooting

### ❌ "No tools available"
- Check Genie space exists and has data
- Verify MCP Beta features are enabled
- Test Genie space directly in Databricks UI

### ❌ "Authentication failed"  
- Ensure workspace authentication is configured
- Check agent has access to Genie space
- Verify Claude Sonnet endpoint is available

### ❌ "Tool execution failed"
- Check Genie space has system table data
- Verify queries are returning results
- Test with simpler questions first

## Next Steps

1. **Test in playground** ✅
2. **Refine prompts and responses** 
3. **Add more sophisticated analysis**
4. **Build secure Databricks App**
5. **Deploy for team usage**

This approach gives you the **power of MCP** with **Claude Sonnet intelligence** while keeping everything **secure within Databricks**!