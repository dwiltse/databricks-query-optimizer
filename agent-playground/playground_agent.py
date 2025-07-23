"""
Query Optimization Agent for Databricks AI Playground
Connects to Genie space via MCP for intelligent query analysis
"""

from databricks_agents import ChatCompletionAgent
from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient
import json

# =============================================================================
# AGENT CONFIGURATION FOR AI PLAYGROUND
# =============================================================================

# Your Genie Space Configuration
GENIE_SPACE_ID = "system_table_mcp_test"
workspace_client = WorkspaceClient()
MCP_SERVER_URL = f"{workspace_client.config.host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}"

# System prompt for query optimization
SYSTEM_PROMPT = """You are an expert Databricks Query Optimization Assistant with access to system tables through a Genie space.

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

Always be specific with your recommendations and include expected performance gains."""

# =============================================================================
# MCP TOOL SETUP
# =============================================================================

def get_mcp_tools():
    """Get available tools from the Genie space MCP server"""
    try:
        mcp_client = DatabricksMCPClient(
            server_url=MCP_SERVER_URL,
            workspace_client=workspace_client
        )
        
        tools = mcp_client.list_tools() 
        print(f"✅ Found {len(tools)} MCP tools from Genie space")
        
        # Convert MCP tools to agent framework format
        tool_configs = []
        for tool in tools:
            tool_config = {
                "name": tool.name,
                "description": tool.description or f"Query optimization tool: {tool.name}",
                "function": create_mcp_tool_function(tool.name)
            }
            tool_configs.append(tool_config)
            
        return tool_configs
        
    except Exception as e:
        print(f"❌ Failed to get MCP tools: {e}")
        return []

def create_mcp_tool_function(tool_name):
    """Create a function wrapper for an MCP tool"""
    def mcp_tool_function(**kwargs):
        try:
            mcp_client = DatabricksMCPClient(
                server_url=MCP_SERVER_URL, 
                workspace_client=workspace_client
            )
            
            print(f"🔧 Executing MCP tool: {tool_name}")
            response = mcp_client.call_tool(tool_name, kwargs)
            result = "".join([c.text for c in response.content])
            
            return result
            
        except Exception as e:
            return f"Error executing {tool_name}: {str(e)}"
    
    return mcp_tool_function

# =============================================================================
# AGENT SETUP FOR AI PLAYGROUND
# =============================================================================

# Get MCP tools from Genie space
mcp_tools = get_mcp_tools()

# Create the agent for AI Playground
agent = ChatCompletionAgent(
    system_message=SYSTEM_PROMPT,
    tools=mcp_tools,
    model="databricks-claude-3-7-sonnet"  # Adjust to your endpoint name
)

# =============================================================================
# SAMPLE INTERACTIONS
# =============================================================================

"""
Sample questions to test in AI Playground:

1. "What are the 5 slowest queries in my system today?"

2. "Show me the most expensive queries by DBU cost and suggest optimizations"  

3. "Analyze query patterns - what materialized views should I create?"

4. "I have queries scanning large tables frequently - what indexing strategy do you recommend?"

5. "Calculate the ROI of optimizing my top 3 slowest queries"

The agent will:
- Use MCP to query your Genie space (system_table_mcp_test)
- Analyze system table data with Claude Sonnet
- Provide specific optimization recommendations
- Estimate performance and cost improvements
"""