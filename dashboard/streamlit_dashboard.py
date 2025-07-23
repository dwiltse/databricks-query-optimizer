"""
Query Optimization Dashboard - Clean Streamlit App
Based on working patterns from system_table_chain_of_debates
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time

# Version indicator
APP_VERSION = "2025-01-22-v1-mcp"

# Simple page config
st.set_page_config(
    page_title="Query Optimization Dashboard",
    page_icon="🚀",
    layout="wide"
)

def main():
    """Main dashboard application."""
    
    # Header
    st.title("🚀 Query Optimization Command Center")
    st.write(f"**Version:** {APP_VERSION}")
    st.write("**Status:** Testing Databricks Apps deployment")
    
    # Connection test with MCP
    st.header("🔌 Connection Status")
    
    # Initialize connection status
    workspace_connected = False
    mcp_connected = False
    mcp_client = None
    
    try:
        from databricks.sdk import WorkspaceClient
        workspace_client = WorkspaceClient()
        workspace_connected = True
        st.success("✅ Connected to Databricks workspace")
        st.write(f"**Host:** {workspace_client.config.host}")
        
        # Test Direct Genie API connection (alternative to MCP)
        try:
            import requests
            import json
            
            # Use Direct Genie Conversation API (Public Preview - no MCP needed!)
            genie_space_id = "system_table_mcp_test"
            
            # Test direct API connection
            headers = {
                "Authorization": f"Bearer {workspace_client.config.token}",
                "Content-Type": "application/json"
            }
            
            # Test with List Spaces endpoint first
            list_spaces_url = f"{workspace_client.config.host}/api/2.0/genie/spaces"
            
            st.write("🔍 Testing Direct Genie API connection...")
            st.write(f"**API URL:** {list_spaces_url}")
            
            try:
                response = requests.get(list_spaces_url, headers=headers)
                if response.status_code == 200:
                    spaces = response.json()
                    st.success("✅ Direct Genie API connected!")
                    st.write("**Available Genie Spaces:**")
                    for space in spaces.get('spaces', []):
                        st.write(f"- **{space.get('display_name', 'Unknown')}** (ID: `{space.get('id', 'Unknown')}`)")
                        if space.get('id') == genie_space_id:
                            st.info(f"🎯 Found target space: **{space.get('display_name')}**")
                    
                    genie_connected = True
                    st.success("✅ Ready to use Direct Genie API")
                else:
                    st.error(f"❌ API connection failed: {response.status_code}")
                    st.write(f"Response: {response.text}")
                    genie_connected = False
                    
            except Exception as api_error:
                st.error(f"❌ Direct API connection failed: {api_error}")
                genie_connected = False
            st.write(f"**Genie Space ID:** {genie_space_id}")
            st.info("🎯 Using Direct Genie API (no MCP required!)")
            
        except ImportError as import_error:
            st.warning("⚠️ MCP libraries not installed")
            st.write(f"Import error: {str(import_error)}")
        except Exception as mcp_error:
            st.warning(f"⚠️ MCP connection failed: {str(mcp_error)}")
            st.info("💡 Ensure MCP is enabled in workspace and Genie Space exists")
            
            # Show more details about the error
            with st.expander("🔧 MCP Connection Debug"):
                import traceback
                st.code(traceback.format_exc())
            
    except Exception as e:
        st.error(f"❌ Databricks connection failed: {str(e)}")
        st.info("This may be expected during initial setup")
    
    # Store connection status in session state for use in other sections
    if 'connections' not in st.session_state:
        st.session_state.connections = {}
    
    st.session_state.connections['workspace'] = workspace_connected
    st.session_state.connections['genie'] = genie_connected if 'genie_connected' in locals() else False
    st.session_state.connections['workspace_client'] = workspace_client
    st.session_state.connections['genie_space_id'] = genie_space_id if 'genie_space_id' in locals() else None
    
    # Simple input interface
    st.header("💬 Query Interface")
    
    col1, col2 = st.columns(2)
    
    with col1:
        question = st.text_input(
            "Ask about your queries:", 
            placeholder="What are the slowest queries?"
        )
        
        if st.button("🔍 Analyze", type="primary"):
            if question:
                st.write(f"**Question:** {question}")
                
                # Try Direct Genie API query if connected
                if st.session_state.connections.get('genie') and st.session_state.connections.get('workspace_client'):
                    try:
                        with st.spinner("🤖 Querying Genie Space via MCP..."):
                            mcp_client = st.session_state.connections['mcp_client']
                            
                            # Simplified query format - try direct question first
                            simple_query = f"Using the system_table_mcp_test Genie Space, {question}"
                            
                            st.write("**Debug Info:**")
                            st.write(f"Query: {simple_query}")
                            
                            # First, try to list available tools
                            try:
                                # Check if list_tools method exists
                                if hasattr(mcp_client, 'list_tools'):
                                    tools = mcp_client.list_tools()
                                    st.write("**Available Tools:**", tools)
                                elif hasattr(mcp_client, 'get_tools'):
                                    tools = mcp_client.get_tools()
                                    st.write("**Available Tools:**", tools)
                                else:
                                    st.info("Cannot list available tools - proceeding with query")
                            except Exception as tool_list_error:
                                st.warning(f"Could not list tools: {tool_list_error}")
                            
                            # Use the correct method from Databricks documentation
                            if hasattr(mcp_client, 'call_tool'):
                                # Try different tool names if "query" doesn't work
                                tool_names_to_try = ["query", "ask", "question", "chat", "analyze"]
                                
                                response = None
                                for tool_name in tool_names_to_try:
                                    try:
                                        st.write(f"🔍 Trying tool: **{tool_name}**")
                                        response = mcp_client.call_tool(tool_name, {"question": simple_query})
                                        st.success(f"✅ Successfully used tool: **{tool_name}**")
                                        break
                                    except Exception as tool_error:
                                        st.warning(f"❌ Tool **{tool_name}** failed: {str(tool_error)[:100]}...")
                                        continue
                                
                                if response is None:
                                    st.error("❌ All tool attempts failed.")
                                    st.warning("🚨 **Possible Issues:**")
                                    st.write("1. **MCP Beta Feature**: Databricks MCP is in Beta and requires serverless compute")
                                    st.write("2. **Serverless Compute**: Ensure serverless compute is enabled in your workspace")
                                    st.write("3. **Workspace Settings**: MCP integration might not be enabled for your workspace")
                                    st.write("4. **Genie Space Configuration**: The space may not have proper data sources or instructions")
                                    
                                    st.info("💡 **Next Steps:**")
                                    st.write("- Contact your Databricks admin to enable MCP Beta features")
                                    st.write("- Verify serverless compute is available and enabled")
                                    st.write("- Check that your workspace has MCP integration enabled")
                                    
                                    raise Exception("MCP integration may not be properly enabled")
                            else:
                                st.error("❌ call_tool method not found on MCP client")
                                st.write("Available methods:", [m for m in dir(mcp_client) if not m.startswith('_')])
                                raise Exception("call_tool method not found")
                            
                            st.success("✅ MCP Query completed!")
                            st.subheader("🤖 AI Analysis")
                            st.write(response)
                            st.balloons()
                    
                    except Exception as e:
                        st.error(f"❌ MCP query failed: {str(e)}")
                        
                        # More detailed error info
                        import traceback
                        with st.expander("🔧 Detailed Error Info"):
                            st.code(traceback.format_exc())
                            st.write("**Error Type:**", type(e).__name__)
                            st.write("**MCP Client Info:**", type(mcp_client).__name__)
                        
                        # Try a very simple test query
                        st.write("**Trying simple test query...**")
                        try:
                            # Use the correct Databricks MCP method
                            test_query = "Hello, can you see the system_table_mcp_test Genie Space?"
                            if hasattr(mcp_client, 'call_tool'):
                                test_response = mcp_client.call_tool("query", {"question": test_query})
                            else:
                                st.error("No call_tool method available for test")
                                test_response = None
                                
                            if test_response:
                                st.success("✅ Simple query worked!")
                                st.write("Test response:", test_response)
                        except Exception as test_error:
                            st.error(f"❌ Simple query also failed: {str(test_error)}")
                        
                        st.info("🚧 Using sample data instead")
                        st.balloons()
                
                else:
                    st.info("🚧 MCP not connected - using sample data for demo")
                    st.balloons()
                    
            else:
                st.warning("Please enter a question")
    
    with col2:
        st.write("**Sample Questions:**")
        st.write("• What are the most expensive queries?")
        st.write("• Which queries have SELECT * patterns?") 
        st.write("• Show me optimization opportunities")
        st.write("• What queries run longer than 5 minutes?")
    
    # Sample analytics
    st.header("📊 Sample Analytics")
    
    # Create sample data
    sample_data = {
        "Optimization Type": ["SELECT * Pattern", "Unbounded Sort", "Cartesian Join", "Missing WHERE"],
        "Query Count": [15, 8, 3, 12],
        "Potential Savings": ["40%", "60%", "80%", "35%"],
        "Monthly Cost Impact": ["$1,200", "$800", "$2,000", "$600"]
    }
    
    df = pd.DataFrame(sample_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.dataframe(df, use_container_width=True)
    
    with col2:
        st.bar_chart(df.set_index("Optimization Type")["Query Count"])
    
    # Success metrics
    st.header("📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Potential Monthly Savings", "$4,600", "+15%")
    
    with col2:
        st.metric("Slow Queries", "38", "-5")
    
    with col3:
        st.metric("Teams Affected", "4", "Marketing, Sales, Finance, Ops")
    
    with col4:
        st.metric("Success Rate", "85%", "+12%")
    
    # Footer
    st.divider()
    
    # Dynamic status based on connections
    if st.session_state.connections.get('mcp'):
        st.success("🎯 **MCP Integration Active!** Connected to system_table_mcp_test Genie Space")
        st.caption("✅ Ready for live query optimization analysis")
    elif st.session_state.connections.get('workspace'):
        st.info("🔧 **Databricks Connected** - MCP setup in progress")
        st.caption("Next: Enable MCP access and verify Genie Space exists")
    else:
        st.warning("🚧 **Setup Required** - Databricks connection needed")
        st.caption("Deploy to Databricks Apps to establish connections")

if __name__ == "__main__":
    main()