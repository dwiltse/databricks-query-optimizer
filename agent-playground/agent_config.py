"""
Databricks Agent Framework Configuration for Query Optimization
This configures an agent in the playground to use Genie space via MCP
"""

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse
from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient
import json
import uuid
from typing import Any, List

# =============================================================================
# AGENT CONFIGURATION
# =============================================================================

# System prompt optimized for query optimization analysis
SYSTEM_PROMPT = """You are an expert Databricks Query Optimization Assistant with access to system tables and query performance data through a Genie space.

Your role is to help users:
- Identify slow and expensive queries
- Analyze query performance patterns and trends
- Recommend specific optimization strategies
- Calculate potential cost savings and ROI
- Provide actionable insights for query tuning

When users ask about query performance, use the Genie space tool to:
1. Query system tables for relevant data
2. Analyze patterns and identify issues
3. Provide specific, implementable recommendations
4. Quantify potential improvements when possible

Always provide:
- Clear explanations of performance issues
- Specific optimization recommendations 
- Expected impact (time/cost savings)
- Implementation guidance

Available data includes:
- Query execution history and performance metrics
- Resource usage and costs
- User patterns and frequency analysis
- Warehouse utilization data
"""

# LLM Configuration
LLM_ENDPOINT = "databricks-claude-3-7-sonnet"  # Adjust to your endpoint name

# Workspace configuration
workspace_client = WorkspaceClient()
workspace_host = workspace_client.config.host

# MCP Server Configuration - Your Genie Space
GENIE_SPACE_ID = "system_table_mcp_test"
MCP_SERVER_URL = f"{workspace_host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}"

print(f"🎯 Configured for Genie Space: {GENIE_SPACE_ID}")
print(f"🔗 MCP Server URL: {MCP_SERVER_URL}")

# =============================================================================
# AGENT IMPLEMENTATION
# =============================================================================

class QueryOptimizationAgent(ResponsesAgent):
    """
    Query Optimization Agent using Genie Space via MCP
    
    This agent connects to your Genie space containing system table data
    and provides intelligent query optimization recommendations using Claude Sonnet.
    """
    
    def __init__(self):
        super().__init__()
        self.workspace_client = WorkspaceClient()
        self.mcp_client = DatabricksMCPClient(
            server_url=MCP_SERVER_URL,
            workspace_client=self.workspace_client
        )
        
        # Discover available tools from the Genie space
        try:
            self.available_tools = self.mcp_client.list_tools()
            print(f"✅ Connected to Genie space with {len(self.available_tools)} tools")
            for tool in self.available_tools:
                print(f"   - {tool.name}: {tool.description}")
        except Exception as e:
            print(f"❌ Failed to connect to Genie space: {e}")
            self.available_tools = []
    
    def _format_tool_specs(self):
        """Format MCP tools for OpenAI function calling format"""
        tool_specs = []
        for tool in self.available_tools:
            schema = tool.inputSchema.copy() if tool.inputSchema else {}
            if "properties" not in schema:
                schema["properties"] = {}
            
            spec = {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description or f"Query the Genie space: {tool.name}",
                    "parameters": schema,
                },
            }
            tool_specs.append(spec)
        
        return tool_specs
    
    def _execute_tool(self, tool_name: str, arguments: dict) -> str:
        """Execute an MCP tool and return the result"""
        try:
            print(f"🔧 Executing Genie space tool: {tool_name}")
            print(f"   Arguments: {arguments}")
            
            response = self.mcp_client.call_tool(tool_name, arguments)
            result = "".join([c.text for c in response.content])
            
            print(f"✅ Tool execution completed. Result length: {len(result)} chars")
            return result
            
        except Exception as e:
            error_msg = f"Error executing {tool_name}: {str(e)}"
            print(f"❌ {error_msg}")
            return error_msg
    
    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """Process a query optimization request"""
        print(f"\n🚀 Processing query optimization request")
        
        # Build conversation history
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        
        # Add user input
        for inp in request.input:
            messages.append({"role": "user", "content": inp.content})
            print(f"👤 User: {inp.content}")
        
        # Get available tools
        tool_specs = self._format_tool_specs()
        
        if not tool_specs:
            return ResponsesAgentResponse(
                output=[{
                    "id": uuid.uuid4().hex,
                    "type": "message", 
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "❌ No Genie space tools available. Please check MCP connection."}]
                }]
            )
        
        # Call Claude Sonnet with tool access
        print(f"🤖 Calling Claude Sonnet with {len(tool_specs)} available tools")
        
        client = self.workspace_client.serving_endpoints.get_open_ai_client()
        
        response = client.chat.completions.create(
            model=LLM_ENDPOINT,
            messages=messages,
            tools=tool_specs,
            temperature=0.1,  # Lower temperature for more consistent analysis
        )
        
        choice = response.choices[0].message
        
        # Handle tool calls if any
        if choice.tool_calls:
            print(f"🔧 Claude wants to use {len(choice.tool_calls)} tools")
            
            # Add assistant message with tool calls
            messages.append({
                "role": "assistant", 
                "content": choice.content,
                "tool_calls": [tc.to_dict() for tc in choice.tool_calls]
            })
            
            # Execute each tool
            for tool_call in choice.tool_calls:
                function_name = tool_call.function.name
                try:
                    function_args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError:
                    function_args = {}
                
                # Execute the tool
                tool_result = self._execute_tool(function_name, function_args)
                
                # Add tool result to conversation
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_result
                })
            
            # Get final response after tool execution
            print("🤖 Getting Claude's final analysis...")
            final_response = client.chat.completions.create(
                model=LLM_ENDPOINT,
                messages=messages,
                temperature=0.1
            )
            
            final_content = final_response.choices[0].message.content
        else:
            final_content = choice.content
        
        print(f"✅ Analysis complete. Response length: {len(final_content or '')} chars")
        
        return ResponsesAgentResponse(
            output=[{
                "id": uuid.uuid4().hex,
                "type": "message",
                "role": "assistant", 
                "content": [{"type": "output_text", "text": final_content or "No response generated"}]
            }],
            custom_outputs=request.custom_inputs
        )

# =============================================================================
# REGISTER AGENT FOR PLAYGROUND
# =============================================================================

# Register the agent with MLflow for use in playground
mlflow.models.set_model(QueryOptimizationAgent())

# =============================================================================
# TESTING FUNCTIONS
# =============================================================================

def test_agent_locally():
    """Test the agent locally before deploying to playground"""
    print("🧪 Testing Query Optimization Agent locally")
    print("=" * 60)
    
    agent = QueryOptimizationAgent()
    
    test_questions = [
        "What are the 5 slowest queries in my system?",
        "Show me the most expensive queries by DBU cost",
        "What query patterns should I optimize first?",
        "Analyze my query performance trends over the last week"
    ]
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n📝 Test {i}: {question}")
        print("-" * 50)
        
        try:
            request = ResponsesAgentRequest(
                input=[{"role": "user", "content": question}]
            )
            
            response = agent.predict(request)
            
            for output in response.output:
                if output.get("content"):
                    for content in output["content"]:
                        if content.get("text"):
                            print(content["text"])
                            
        except Exception as e:
            print(f"❌ Error in test {i}: {e}")
        
        print("\n" + "=" * 60)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_agent_locally()
    else:
        print("🎯 Query Optimization Agent configured for Databricks Playground")
        print(f"   Genie Space: {GENIE_SPACE_ID}")
        print(f"   MCP URL: {MCP_SERVER_URL}")
        print("\nTo test locally: python agent_config.py test")
        print("To deploy: Use this file in Databricks Agent Framework")