#!/usr/bin/env python3
"""
Query Optimization MCP Agent using Databricks Managed MCP
Connects to Genie space for intelligent query analysis with Claude Sonnet
"""

import json
import uuid
from typing import Any, Callable, List
from pydantic import BaseModel

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient

# =============================================================================
# CONFIGURATION - Update these for your environment
# =============================================================================

# LLM Configuration
LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"

# System prompt optimized for query optimization analysis
SYSTEM_PROMPT = """You are an expert Databricks query optimization analyst with access to system tables and query performance data through Genie spaces. 

Your capabilities include:
- Analyzing query performance patterns and bottlenecks
- Identifying expensive queries and resource usage issues  
- Recommending specific optimization strategies (indexing, partitioning, query rewriting)
- Calculating potential cost savings and performance improvements
- Providing actionable insights based on execution history

When analyzing queries, focus on:
1. Execution time patterns and trends
2. Resource consumption (CPU, memory, I/O)
3. Cost analysis and optimization opportunities
4. Query patterns that could benefit from materialized views or caching
5. Specific, implementable recommendations with expected impact

Use the Genie space tools to query system tables and provide data-driven insights."""

# Authentication - Set to your Databricks CLI profile name
DATABRICKS_CLI_PROFILE = "YOUR_DATABRICKS_CLI_PROFILE"
assert DATABRICKS_CLI_PROFILE != "YOUR_DATABRICKS_CLI_PROFILE", \
    "Please set DATABRICKS_CLI_PROFILE to your actual Databricks CLI profile name"

# Initialize workspace client
workspace_client = WorkspaceClient(profile=DATABRICKS_CLI_PROFILE)
host = workspace_client.config.host

# MCP Server URLs - Configure based on your setup
MANAGED_MCP_SERVER_URLS = [
    # Built-in AI tools (Python executor, etc.)
    f"{host}/api/2.0/mcp/functions/system/ai",
    
    # Your Genie space for query optimization data
    f"{host}/api/2.0/mcp/genie/system_table_mcp_test",
    
    # Add more Genie spaces if you have them:
    # f"{host}/api/2.0/mcp/genie/genie_space_query_optimization_prod",
    
    # Add Unity Catalog function schemas if needed:
    # f"{host}/api/2.0/mcp/functions/mcp/query_optimization", 
]

# Custom MCP Servers (if you have any hosted on Databricks Apps)
CUSTOM_MCP_SERVER_URLS = []

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _to_chat_messages(msg: dict[str, Any]) -> List[dict]:
    """Convert ResponsesAgent message format to ChatCompletions format."""
    msg_type = msg.get("type")
    if msg_type == "function_call":
        return [
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": msg["call_id"],
                        "type": "function",
                        "function": {
                            "name": msg["name"],
                            "arguments": msg["arguments"],
                        },
                    }
                ],
            }
        ]
    elif msg_type == "message" and isinstance(msg["content"], list):
        return [
            {
                "role": "assistant" if msg["role"] == "assistant" else msg["role"],
                "content": content["text"],
            }
            for content in msg["content"]
        ]
    elif msg_type == "function_call_output":
        return [
            {
                "role": "tool",
                "content": msg["output"],
                "tool_call_id": msg["tool_call_id"],
            }
        ]
    else:
        # Fallback for plain message format
        return [
            {
                k: v
                for k, v in msg.items()
                if k in ("role", "content", "name", "tool_calls", "tool_call_id")
            }
        ]

# =============================================================================
# MCP TOOL MANAGEMENT
# =============================================================================

def _make_exec_fn(server_url: str, tool_name: str, ws: WorkspaceClient) -> Callable[..., str]:
    """Create execution function for an MCP tool."""
    def exec_fn(**kwargs):
        print(f"🔧 Executing tool '{tool_name}' from {server_url}")
        print(f"   Arguments: {kwargs}")
        
        mcp_client = DatabricksMCPClient(server_url=server_url, workspace_client=ws)
        response = mcp_client.call_tool(tool_name, kwargs)
        result = "".join([c.text for c in response.content])
        
        print(f"✅ Tool execution completed. Result length: {len(result)} chars")
        return result
    
    return exec_fn

class ToolInfo(BaseModel):
    name: str
    spec: dict
    exec_fn: Callable

def _fetch_tool_infos(ws: WorkspaceClient, server_url: str) -> List[ToolInfo]:
    """Fetch available tools from an MCP server."""
    print(f"🔍 Discovering tools from MCP server: {server_url}")
    
    try:
        infos: List[ToolInfo] = []
        mcp_client = DatabricksMCPClient(server_url=server_url, workspace_client=ws)
        mcp_tools = mcp_client.list_tools()
        
        for t in mcp_tools:
            schema = t.inputSchema.copy() if t.inputSchema else {}
            if "properties" not in schema:
                schema["properties"] = {}
            
            spec = {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description or f"Tool: {t.name}",
                    "parameters": schema,
                },
            }
            
            infos.append(
                ToolInfo(
                    name=t.name, 
                    spec=spec, 
                    exec_fn=_make_exec_fn(server_url, t.name, ws)
                )
            )
        
        print(f"✅ Found {len(infos)} tools: {[info.name for info in infos]}")
        return infos
        
    except Exception as e:
        print(f"❌ Failed to fetch tools from {server_url}: {e}")
        return []

# =============================================================================
# QUERY OPTIMIZATION AGENT
# =============================================================================

class QueryOptimizationMCPAgent(ResponsesAgent):
    """
    Intelligent Query Optimization Agent using MCP tools and Claude Sonnet.
    
    This agent can:
    - Connect to Databricks Genie spaces with system table data
    - Use Claude Sonnet for advanced analysis and recommendations
    - Execute Python code for complex calculations
    - Provide actionable optimization insights
    """
    
    def _call_llm(self, history: List[dict], ws: WorkspaceClient, tool_infos: List[ToolInfo]):
        """Call Claude Sonnet with current conversation history and available tools."""
        client = ws.serving_endpoints.get_open_ai_client()
        
        # Convert message history to ChatCompletions format
        flat_msgs = []
        for msg in history:
            flat_msgs.extend(_to_chat_messages(msg))
        
        print(f"🤖 Calling Claude Sonnet with {len(flat_msgs)} messages and {len(tool_infos)} tools")
        
        return client.chat.completions.create(
            model=LLM_ENDPOINT_NAME,
            messages=flat_msgs,
            tools=[ti.spec for ti in tool_infos],
            temperature=0.1,  # Lower temperature for more consistent analysis
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """Process a query optimization request."""
        ws = WorkspaceClient(profile=DATABRICKS_CLI_PROFILE)

        # Build conversation history
        history: List[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
        
        for inp in request.input:
            history.append(inp.model_dump())
            print(f"👤 User: {inp.content}")

        # Discover all available tools from MCP servers
        print("\n🔧 Discovering MCP tools...")
        tool_infos = []
        for mcp_server_url in (MANAGED_MCP_SERVER_URLS + CUSTOM_MCP_SERVER_URLS):
            server_tools = _fetch_tool_infos(ws, mcp_server_url)
            tool_infos.extend(server_tools)
        
        tools_dict = {tool_info.name: tool_info for tool_info in tool_infos}
        print(f"📋 Total tools available: {len(tool_infos)}")

        # Initial LLM call
        print("\n🤖 Getting Claude Sonnet's analysis...")
        llm_resp = self._call_llm(history, ws, tool_infos)
        raw_choice = llm_resp.choices[0].message.to_dict()
        raw_choice["id"] = uuid.uuid4().hex
        history.append(raw_choice)

        # Process tool calls if any
        tool_calls = raw_choice.get("tool_calls") or []
        if tool_calls:
            print(f"\n🔧 Claude wants to use {len(tool_calls)} tools")
            
            # Execute each tool call
            for fc in tool_calls:
                name = fc["function"]["name"]
                try:
                    args = json.loads(fc["function"]["arguments"])
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid JSON in tool arguments: {e}")
                    args = {}
                
                try:
                    if name in tools_dict:
                        tool_info = tools_dict[name]
                        result = tool_info.exec_fn(**args)
                    else:
                        result = f"Error: Tool '{name}' not found. Available tools: {list(tools_dict.keys())}"
                except Exception as e:
                    result = f"Error executing {name}: {str(e)}"
                    print(f"❌ Tool execution failed: {e}")

                # Add tool result to history
                history.append({
                    "type": "function_call_output",
                    "role": "tool",
                    "id": uuid.uuid4().hex,
                    "tool_call_id": fc["id"],
                    "output": result,
                })

            # Get final response from Claude after tool execution
            print("\n🤖 Getting Claude's final analysis...")
            followup = self._call_llm(history, ws, tool_infos=[]).choices[0].message.to_dict()
            followup["id"] = uuid.uuid4().hex

            assistant_text = followup.get("content", "")
        else:
            assistant_text = raw_choice.get("content", "")

        print(f"\n✅ Analysis complete. Response length: {len(assistant_text)} chars")
        
        return ResponsesAgentResponse(
            output=[
                {
                    "id": uuid.uuid4().hex,
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": assistant_text}],
                }
            ],
            custom_outputs=request.custom_inputs,
        )

# =============================================================================
# MAIN EXECUTION
# =============================================================================

# Register the agent with MLflow
mlflow.models.set_model(QueryOptimizationMCPAgent())

def test_agent():
    """Test the agent with sample query optimization questions."""
    print("🚀 Testing Query Optimization MCP Agent")
    print("=" * 60)
    
    agent = QueryOptimizationMCPAgent()
    
    test_questions = [
        "What are the 5 slowest queries in the system in the last 24 hours?",
        "Show me queries that consume the most DBUs and suggest optimizations",
        "What query patterns appear most frequently and could benefit from materialized views?",
        "Calculate the potential cost savings from optimizing the top 10 most expensive queries",
    ]
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n📝 Test Question {i}: {question}")
        print("-" * 50)
        
        try:
            req = ResponsesAgentRequest(
                input=[{"role": "user", "content": question}]
            )
            resp = agent.predict(req)
            
            for item in resp.output:
                if item.get("content"):
                    for content_item in item["content"]:
                        if content_item.get("text"):
                            print(content_item["text"])
                            
        except Exception as e:
            print(f"❌ Error testing question {i}: {e}")
        
        print("\n" + "=" * 60)

if __name__ == "__main__":
    # Check if running in test mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_agent()
    else:
        print("🚀 Query Optimization MCP Agent initialized")
        print("Use with MLflow serving or call test_agent() for testing")
        print("\nTo test: python query_optimization_mcp_agent.py test")