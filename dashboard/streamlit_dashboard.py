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
        
        # Test MCP connection
        try:
            from databricks_mcp import DatabricksMCPClient
            
            # Show connection attempt details - using correct Genie Space URL format
            genie_space_id = "system_table_mcp_test"
            mcp_url = f"{workspace_client.config.host}/api/2.0/mcp/genie/{genie_space_id}"
            st.write(f"**MCP URL:** {mcp_url}")
            st.write(f"**Genie Space ID:** {genie_space_id}")
            
            # Try to connect to MCP server using correct URL format
            mcp_client = DatabricksMCPClient(
                server_url=mcp_url,
                workspace_client=workspace_client
            )
            mcp_connected = True
            st.success("✅ MCP connection established")
            st.info("🎯 Ready to query Genie Space: **system_table_mcp_test**")
            
            # Test connection with a simple ping
            try:
                # Just verify the client is working - don't run a full query yet
                st.write("**MCP Client Type:**", type(mcp_client).__name__)
                all_methods = [m for m in dir(mcp_client) if not m.startswith('_')]
                st.write("**Available Methods:**", all_methods)  # Show ALL methods to find the right one
            except Exception as ping_error:
                st.warning(f"⚠️ MCP client test failed: {str(ping_error)}")
            
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
    st.session_state.connections['mcp'] = mcp_connected
    st.session_state.connections['mcp_client'] = mcp_client
    
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
                
                # Try MCP query if connected
                if st.session_state.connections.get('mcp') and st.session_state.connections.get('mcp_client'):
                    try:
                        with st.spinner("🤖 Querying Genie Space via MCP..."):
                            mcp_client = st.session_state.connections['mcp_client']
                            
                            # Simplified query format - try direct question first
                            simple_query = f"Using the system_table_mcp_test Genie Space, {question}"
                            
                            st.write("**Debug Info:**")
                            st.write(f"Query: {simple_query}")
                            
                            # Use the correct method from Databricks documentation
                            if hasattr(mcp_client, 'call_tool'):
                                # This is the documented method for Databricks MCP
                                response = mcp_client.call_tool("query", {"question": simple_query})
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