"""
MCP Connection Manager - The heart of our query optimization app!
Uses the proven pattern from your playground success
"""

from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient
import json
import streamlit as st
from datetime import datetime, timedelta

class MCPConnectionManager:
    """Manages connection to Genie space via MCP - your proven working pattern!"""
    
    def __init__(self, genie_space_id="system_table_mcp_test"):
        self.genie_space_id = genie_space_id
        self.workspace_client = None
        self.mcp_client = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize MCP client with your playground-proven pattern"""
        try:
            # Same pattern that worked in playground!
            self.workspace_client = WorkspaceClient()
            mcp_url = f"{self.workspace_client.config.host}/api/2.0/mcp/genie/{self.genie_space_id}"
            
            self.mcp_client = DatabricksMCPClient(
                server_url=mcp_url,
                workspace_client=self.workspace_client
            )
            
            print(f"🎯 Connected to Genie space: {self.genie_space_id}")
            print(f"🔗 MCP URL: {mcp_url}")
            
        except Exception as e:
            print(f"❌ MCP connection failed: {e}")
            self.mcp_client = None
    
    def test_connection(self):
        """Test if MCP connection is working with specific error diagnosis"""
        if not self.mcp_client:
            return {
                "status": "error", 
                "error_type": "initialization_failed",
                "message": "MCP client not initialized - check workspace authentication",
                "troubleshooting": [
                    "Verify you're running in a Databricks environment",
                    "Check workspace authentication is working",
                    "Ensure databricks-mcp package is installed"
                ]
            }
        
        try:
            # Try to list available tools
            tools = self.mcp_client.list_tools()
            return {
                "status": "success", 
                "message": f"Connected! Found {len(tools)} tools",
                "tools": [{"name": tool.name, "description": tool.description} for tool in tools]
            }
        except Exception as e:
            error_msg = str(e).lower()
            
            # Specific error diagnosis
            if "404" in error_msg or "not found" in error_msg:
                return {
                    "status": "error",
                    "error_type": "genie_space_not_found", 
                    "message": f"Genie space '{self.genie_space_id}' not found",
                    "troubleshooting": [
                        f"Create Genie space with ID: {self.genie_space_id}",
                        "Verify the space name is correct",
                        "Check you have access to the space"
                    ]
                }
            elif "403" in error_msg or "forbidden" in error_msg:
                return {
                    "status": "error",
                    "error_type": "access_denied",
                    "message": "Access denied to Genie space",
                    "troubleshooting": [
                        "Request access to the Genie space",
                        "Check your workspace permissions",
                        "Verify you're authenticated correctly"
                    ]
                }
            elif "401" in error_msg or "unauthorized" in error_msg:
                return {
                    "status": "error", 
                    "error_type": "authentication_failed",
                    "message": "Authentication failed",
                    "troubleshooting": [
                        "Check workspace authentication",
                        "Verify your token/credentials",
                        "Try re-authenticating to Databricks"
                    ]
                }
            elif "beta" in error_msg or "not enabled" in error_msg:
                return {
                    "status": "error",
                    "error_type": "mcp_not_enabled", 
                    "message": "MCP Beta features not enabled",
                    "troubleshooting": [
                        "Contact your Databricks admin",
                        "Request MCP Beta feature enablement", 
                        "Ensure serverless compute is enabled"
                    ]
                }
            else:
                return {
                    "status": "error",
                    "error_type": "unknown",
                    "message": f"Connection test failed: {str(e)}",
                    "troubleshooting": [
                        "Check Databricks workspace connectivity",
                        "Verify MCP service is running",
                        "Review error details for specific issues"
                    ]
                }
    
    def query_genie_space(self, question):
        """Query the Genie space - same as playground!"""
        if not self.mcp_client:
            return {"error": "MCP client not connected"}
        
        try:
            print(f"🤖 Querying Genie: {question}")
            response = self.mcp_client.call_tool("query", {"question": question})
            
            # Extract text content from response
            result = "".join([c.text for c in response.content])
            
            return {"success": True, "data": result}
            
        except Exception as e:
            return {"error": f"Query failed: {str(e)}"}
    
    def get_worst_queries(self, hours_back=24, min_duration_seconds=30, limit=10):
        """Find the worst performing queries - the money maker!"""
        
        query_text = f"""
        Find the {limit} worst performing queries in the last {hours_back} hours.
        
        Criteria:
        - Execution time longer than {min_duration_seconds} seconds
        - Include query_id, statement_text, execution_duration_ms, user_name, warehouse_id
        - Order by execution time (slowest first)
        - Include the actual SQL text so we can optimize it
        
        Format the results clearly with each query's performance metrics.
        """
        
        return self.query_genie_space(query_text)
    
    def get_expensive_queries(self, hours_back=24, limit=10):
        """Find the most expensive queries by DBU cost"""
        
        query_text = f"""
        Find the {limit} most expensive queries by DBU consumption in the last {hours_back} hours.
        
        Include:
        - Query ID and SQL statement text
        - DBU cost and execution time
        - User and warehouse information
        - Data volume processed (rows_read, bytes_read)
        
        Order by cost (most expensive first).
        Show the actual SQL so we can analyze and optimize it.
        """
        
        return self.query_genie_space(query_text)
    
    def get_query_details(self, query_id):
        """Get detailed analysis for a specific query"""
        
        query_text = f"""
        For query_id '{query_id}', provide detailed analysis:
        
        Performance metrics:
        - Execution time, queue time, compilation time
        - Resource usage (CPU, memory, I/O)
        - Data scan statistics
        
        Query structure:
        - Tables accessed and JOIN patterns
        - WHERE clause complexity
        - Aggregation and sorting operations
        
        Optimization opportunities:
        - Missing indexes that could help
        - Inefficient JOIN orders
        - Partition pruning opportunities
        - Caching potential
        
        Provide specific, actionable recommendations.
        """
        
        return self.query_genie_space(query_text)
    
    def analyze_query_patterns(self, hours_back=168):  # 1 week default
        """Analyze overall query patterns for systemic issues"""
        
        query_text = f"""
        Analyze query patterns over the last {hours_back} hours to identify:
        
        Common performance issues:
        - Most frequently scanned tables
        - Repeated inefficient query patterns
        - Users with consistently slow queries
        
        Optimization opportunities:
        - Tables that would benefit from indexing
        - Common JOINs that could use materialized views
        - Partitioning strategies for large tables
        
        Provide a prioritized list of systemic improvements that would impact multiple queries.
        """
        
        return self.query_genie_space(query_text)

# Streamlit integration helpers
@st.cache_resource
def get_mcp_manager():
    """Get MCP manager with resource caching for Streamlit (maintains connections)"""
    return MCPConnectionManager()

def cleanup_mcp_connections():
    """Cleanup MCP connections when app shuts down"""
    try:
        # Clear streamlit cache to trigger cleanup
        st.cache_resource.clear()
        print("🧹 MCP connections cleaned up")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

def display_mcp_status():
    """Display MCP connection status in Streamlit"""
    mcp = get_mcp_manager()
    status = mcp.test_connection()
    
    if status["status"] == "success":
        st.success(f"✅ MCP Connected: {status['message']}")
        
        with st.expander("Available Tools"):
            for tool in status.get("tools", []):
                st.write(f"**{tool['name']}**: {tool['description']}")
    else:
        st.error(f"❌ MCP Connection Failed: {status['message']}")
        st.write("**Troubleshooting:**")
        st.write("- Check that your Genie space 'system_table_mcp_test' exists")
        st.write("- Verify MCP Beta features are enabled")
        st.write("- Ensure you have access to the Genie space")
    
    return status["status"] == "success"

# Test function for development
def test_mcp_connection():
    """Test the MCP connection - run this to validate setup"""
    print("🧪 Testing MCP Connection...")
    
    mcp = MCPConnectionManager()
    
    # Test 1: Connection
    status = mcp.test_connection()
    print(f"Connection Status: {status}")
    
    if status["status"] != "success":
        print("❌ Connection failed - check your setup!")
        return False
    
    # Test 2: Simple query
    print("\n🤖 Testing simple query...")
    result = mcp.query_genie_space("How many queries were executed in the last hour?")
    print(f"Query Result: {result}")
    
    # Test 3: Worst queries
    print("\n🐌 Testing worst queries detection...")
    worst = mcp.get_worst_queries(hours_back=24, limit=3)
    print(f"Worst Queries: {worst}")
    
    print("\n✅ MCP Connection Test Complete!")
    return True

if __name__ == "__main__":
    # Run connection test
    test_mcp_connection()