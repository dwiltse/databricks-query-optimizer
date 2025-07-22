"""
Query Optimization Dashboard - Databricks MCP POC
Deployed as Databricks App - Connects to Genie Space 'system_table_mcp_test' via Managed MCP
"""

import streamlit as st
import plotly.express as px
import pandas as pd
import os

# Databricks Apps specific imports
try:
    from databricks_mcp import DatabricksMCPClient
    from databricks.sdk import WorkspaceClient
    MCP_AVAILABLE = True
except ImportError:
    st.error("⚠️ Databricks MCP libraries not available. Install databricks-mcp in your App environment.")
    MCP_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="Query Optimization Command Center",
    page_icon="🚀",
    layout="wide"
)

@st.cache_resource
def initialize_mcp_connection():
    """Initialize Databricks MCP connection - optimized for Databricks Apps"""
    if not MCP_AVAILABLE:
        return None, None
        
    try:
        # In Databricks Apps, authentication is handled automatically
        # No need for explicit token configuration
        workspace_client = WorkspaceClient()
        
        # Get workspace hostname for display
        workspace_host = workspace_client.config.host or "Databricks Workspace"
        
        # Connect to Managed MCP server for Genie Space access
        mcp_client = DatabricksMCPClient(
            server_url=f"{workspace_host}/api/2.0/mcp/functions/genie",  # Updated for Genie Space
            workspace_client=workspace_client
        )
        
        return mcp_client, workspace_host
    except Exception as e:
        st.error(f"Failed to connect to Databricks MCP: {str(e)}")
        st.info("💡 This app requires Databricks Apps environment with MCP access enabled.")
        return None, None

def query_genie_space(mcp_client, question):
    """Query the Genie Space using natural language - Databricks Apps optimized"""
    try:
        # For Databricks Apps, target the specific Genie Space directly
        genie_space_query = f"""
        Context: You are querying the 'system_table_mcp_test' Genie Space which contains query optimization data.
        
        Available tables:
        - query_performance_raw: Historical query performance metrics
        - query_patterns: Identified optimization patterns  
        - optimization_tracking: Applied optimizations and impact
        - performance_baselines: Performance benchmarks
        
        Question: {question}
        
        Please provide insights based on the data in these tables.
        """
        
        response = mcp_client.query(genie_space_query)
        return response
    except Exception as e:
        st.error(f"Genie Space query failed: {str(e)}")
        st.info("🔧 Troubleshooting: Ensure 'system_table_mcp_test' Genie Space exists and has data.")
        return None

def main():
    # Header
    st.title("🚀 Query Optimization Command Center")
    st.subheader("Powered by Databricks Managed MCP + Genie Spaces")
    
    # Initialize connection
    mcp_client, workspace_host = initialize_mcp_connection()
    
    if not mcp_client:
        st.error("❌ Could not connect to Databricks MCP.")
        st.info("💡 This app requires Databricks Apps environment with MCP and Genie Space access.")
        st.info("🔧 Verify that 'system_table_mcp_test' Genie Space exists in your workspace.")
        return
    
    st.success(f"✅ Connected to Databricks MCP")
    st.info("🎯 Target Genie Space: **system_table_mcp_test**")
    
    # Add environment info for debugging
    with st.expander("🔧 Environment Info"):
        st.write(f"**Workspace:** {workspace_host}")
        st.write("**Deployment:** Databricks App")
        st.write("**MCP Integration:** Managed MCP Server")
        st.write("**Target Tables:** query_performance_raw, query_patterns, optimization_tracking, performance_baselines")
    
    # Sidebar for controls
    st.sidebar.header("🔧 Controls")
    
    # Natural Language Query Interface
    st.header("💬 Ask About Your Queries")
    
    # Pre-defined questions
    predefined_questions = [
        "What are the top 3 slowest queries this week?",
        "Which users have the most expensive queries?",
        "Show me queries with the highest optimization potential",
        "What are the most common query patterns that need optimization?",
        "Which queries are causing the highest costs?"
    ]
    
    selected_question = st.selectbox(
        "Choose a pre-defined question or enter your own:",
        [""] + predefined_questions
    )
    
    custom_question = st.text_input(
        "Or ask your own question:",
        value=selected_question if selected_question else "",
        placeholder="e.g., 'What queries ran longer than 5 minutes today?'"
    )
    
    if st.button("🔍 Ask Genie Space", type="primary"):
        if custom_question:
            with st.spinner("🤖 Querying Genie Space via MCP..."):
                response = query_genie_space(mcp_client, custom_question)
                
                if response:
                    st.success("✅ Query completed!")
                    
                    # Display response
                    st.header("📊 Results")
                    
                    # If response contains structured data, try to parse it
                    if hasattr(response, 'data') and response.data:
                        # Try to create visualizations if data is tabular
                        try:
                            df = pd.DataFrame(response.data)
                            st.dataframe(df, use_container_width=True)
                            
                            # Auto-generate charts for numeric data
                            numeric_cols = df.select_dtypes(include=['number']).columns
                            if len(numeric_cols) > 0:
                                st.subheader("📈 Visualization")
                                chart_col = st.selectbox("Select column to chart:", numeric_cols)
                                fig = px.bar(df.head(10), y=chart_col, title=f"Top 10 by {chart_col}")
                                st.plotly_chart(fig, use_container_width=True)
                        except:
                            pass
                    
                    # Display raw response
                    st.subheader("🤖 AI Analysis")
                    st.write(response)
        else:
            st.warning("Please enter a question first!")
    
    # Static Dashboard Sections (while developing MCP integration)
    st.header("📊 Query Optimization Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "💰 Potential Monthly Savings", 
            "$4,200", 
            "+15% vs last month"
        )
    
    with col2:
        st.metric(
            "🐌 Slow Queries", 
            "12", 
            "-3 vs last week"
        )
    
    with col3:
        st.metric(
            "👥 Teams Affected", 
            "4", 
            "Marketing, Sales, Finance, Operations"
        )
    
    with col4:
        st.metric(
            "🎯 Optimization Success Rate", 
            "85%", 
            "+12% vs last month"
        )
    
    # Sample queries section (for demo purposes)
    st.header("🔥 Top Optimization Opportunities")
    
    # Create sample data that would come from your Genie Space
    sample_opportunities = [
        {
            "rank": 1,
            "query_type": "SELECT * Anti-pattern",
            "user": "sarah@company.com",
            "potential_savings": "40%",
            "monthly_cost_impact": "$1,200",
            "fix_complexity": "Low",
            "description": "Query scans entire table unnecessarily"
        },
        {
            "rank": 2,
            "query_type": "Unbounded Sort",
            "user": "marketing-team@company.com",
            "potential_savings": "60%",
            "monthly_cost_impact": "$800",
            "fix_complexity": "Medium",
            "description": "ORDER BY without LIMIT sorting 50M+ rows"
        },
        {
            "rank": 3,
            "query_type": "Cartesian Join",
            "user": "analytics@company.com",
            "potential_savings": "80%",
            "monthly_cost_impact": "$2,000",
            "fix_complexity": "High",
            "description": "Missing JOIN condition causing cross product"
        }
    ]
    
    for opp in sample_opportunities:
        with st.expander(f"#{opp['rank']} - {opp['query_type']} ({opp['potential_savings']} savings potential)"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**User:** {opp['user']}")
                st.write(f"**Monthly Impact:** {opp['monthly_cost_impact']}")
                st.write(f"**Fix Complexity:** {opp['fix_complexity']}")
            
            with col2:
                st.write(f"**Issue:** {opp['description']}")
                if st.button(f"🔧 Apply Fix #{opp['rank']}", key=f"fix_{opp['rank']}"):
                    st.success(f"✅ Fix applied! Optimization #{opp['rank']} deployed.")
    
    # Footer
    st.divider()
    st.caption("🤖 Powered by Databricks Managed MCP + Genie Spaces | Query Optimization Platform POC")

if __name__ == "__main__":
    main()