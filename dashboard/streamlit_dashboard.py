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
            
            # Try to connect to MCP server
            mcp_client = DatabricksMCPClient(
                server_url=f"{workspace_client.config.host}/api/2.0/mcp/functions/genie",
                workspace_client=workspace_client
            )
            mcp_connected = True
            st.success("✅ MCP connection established")
            st.info("🎯 Ready to query Genie Space: **system_table_mcp_test**")
            
        except ImportError:
            st.warning("⚠️ MCP libraries not installed")
        except Exception as mcp_error:
            st.warning(f"⚠️ MCP connection failed: {str(mcp_error)}")
            st.info("💡 Ensure MCP is enabled in workspace and Genie Space exists")
            
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
                            
                            # Query the system_table_mcp_test Genie Space
                            genie_query = f"""
                            Context: You are querying the 'system_table_mcp_test' Genie Space which contains query optimization data.
                            
                            Available tables:
                            - query_performance_raw: Historical query performance metrics
                            - query_patterns: Identified optimization patterns  
                            - optimization_tracking: Applied optimizations and impact
                            - performance_baselines: Performance benchmarks
                            
                            Question: {question}
                            
                            Please provide insights based on the data in these tables.
                            """
                            
                            response = mcp_client.query(genie_query)
                            
                            st.success("✅ MCP Query completed!")
                            st.subheader("🤖 AI Analysis")
                            st.write(response)
                            st.balloons()
                    
                    except Exception as e:
                        st.error(f"❌ MCP query failed: {str(e)}")
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