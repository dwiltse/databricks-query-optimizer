# Query Optimization Agent - AI Playground Setup

## 🎯 Overview
This creates a query optimization agent in Databricks AI Playground that uses MCP to connect to your Genie space and provides intelligent recommendations using Claude Sonnet.

## 📋 Prerequisites
- ✅ Databricks workspace with AI Playground enabled
- ✅ Your `system_table_mcp_test` Genie space is running
- ✅ Claude Sonnet endpoint available in your workspace
- ✅ MCP Beta features enabled (for managed MCP servers)

## 🚀 Setup Steps

### Step 1: Navigate to AI Playground
1. Go to your Databricks workspace
2. Navigate to **AI Playground** (usually in sidebar or ML section)
3. Click **"Create New Agent"** or **"New Chat Agent"**

### Step 2: Configure the Agent

**Agent Name**: `Query Optimization Assistant`

**Description**: `AI assistant for analyzing query performance and providing optimization recommendations using system tables`

**System Message**:
```
You are an expert Databricks Query Optimization Assistant with access to system tables through a Genie space.

Your capabilities:
- Analyze query performance from system.query.history
- Identify slow and expensive queries  
- Recommend specific optimizations (indexing, partitioning, query rewriting)
- Calculate cost savings and performance improvements
- Provide actionable implementation steps

When users ask about query performance:
1. Use the Genie space to query system tables
2. Analyze the data for patterns and issues
3. Provide specific, implementable recommendations
4. Estimate the impact of optimizations

Always be specific with your recommendations and include expected performance gains.
```

**Model**: `databricks-claude-3-7-sonnet` (or your Claude endpoint name)

### Step 3: Add MCP Tool Configuration

**Tool Type**: `MCP Server`

**MCP Server URL**: 
```
https://YOUR_WORKSPACE_HOST/api/2.0/mcp/genie/system_table_mcp_test
```

**Authentication**: `Workspace Authentication` (use your current session)

**Tool Name**: `genie_query_optimization`

**Tool Description**: `Query system tables for performance analysis and optimization recommendations`

### Step 4: Test the Configuration

Click **"Test Connection"** to verify:
- ✅ MCP server connection works
- ✅ Genie space is accessible  
- ✅ Tools are discovered
- ✅ Claude Sonnet endpoint responds

## 🧪 Test Questions

Once configured, test with these questions:

### Basic Query Analysis
```
"What are the 5 slowest queries in my system in the last 24 hours?"
```

### Cost Optimization
```
"Show me the most expensive queries by DBU consumption and suggest specific optimizations"
```

### Pattern Analysis  
```
"Analyze my query patterns over the last week - what materialized views should I create?"
```

### Specific Optimization
```
"I have a query that scans 1TB of data and takes 10 minutes. What optimization strategies should I try?"
```

### ROI Analysis
```
"Calculate the potential cost savings from optimizing my top 3 most expensive queries"
```

## 📊 Expected Behavior

The agent should:

1. **🔧 Use MCP Tool**: Call your Genie space to query system tables
2. **🤖 Analyze Data**: Claude processes the performance data
3. **💡 Provide Recommendations**: Specific, actionable optimization advice
4. **📈 Estimate Impact**: Quantify expected improvements when possible

### Sample Interaction:
```
👤 You: "What are my slowest queries?"

🤖 Agent: "Let me analyze your query performance data..."
[Uses MCP to query Genie space]

🤖 Agent: "I found 5 queries with execution times > 2 minutes:

1. Customer Analytics Query (4.2 min avg)
   - Scans 500GB from orders table
   - Recommendation: Add index on customer_id + date columns
   - Expected improvement: 60% faster execution
   
2. Product Report Query (3.8 min avg)  
   - Multiple JOINs without proper indexing
   - Recommendation: Create materialized view for product catalog
   - Expected improvement: 70% faster, 40% less cost
   
[continues with specific analysis...]"
```

## 🔧 Troubleshooting

### ❌ "MCP server not found"
- Verify your Genie space ID is correct
- Check that MCP Beta features are enabled
- Ensure Genie space is running and has data

### ❌ "Authentication failed"
- Use workspace authentication (not personal tokens)
- Verify you have access to the Genie space
- Check that serverless compute is enabled

### ❌ "No tools discovered"  
- Test your Genie space directly in Databricks UI
- Verify the space has system table data configured
- Check MCP server URL format is correct

### ❌ "Claude endpoint not available"
- Verify the endpoint name matches your workspace
- Check that Claude Sonnet is enabled for your workspace
- Try with a different model if needed

## 🎉 Success Criteria

You'll know it's working when:
- ✅ Agent can query your system tables via Genie space
- ✅ Claude provides intelligent analysis of query performance  
- ✅ Recommendations are specific and actionable
- ✅ Performance and cost impacts are estimated
- ✅ All data stays within Databricks (secure!)

## 🔄 Next Steps

Once the playground demo works:
1. **Showcase the capability** to stakeholders
2. **Collect feedback** on the types of recommendations  
3. **Refine the system prompt** for better responses
4. **Build the production app** with static optimization pipeline

This gives you the **perfect demo** of MCP + Claude Sonnet analyzing your actual query data!