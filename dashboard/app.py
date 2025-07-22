"""
Minimal Databricks Apps Entry Point
Simplified version without advanced Streamlit features that cause context errors
"""

def main():
    # Import inside main to avoid context issues
    import streamlit as st
    
    # Basic page setup
    st.title("🚀 Query Optimization Dashboard")
    st.write("**Status**: Testing Databricks Apps deployment")
    
    # Simple connection test
    try:
        from databricks.sdk import WorkspaceClient
        workspace_client = WorkspaceClient()
        st.success("✅ Connected to Databricks workspace")
        st.write(f"**Workspace Host**: {workspace_client.config.host}")
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)}")
        st.write("This is expected if MCP libraries aren't installed")
    
    # Simple form without session state
    st.header("💬 Test Query Interface")
    
    question = st.text_input("Ask about your queries:", 
                            placeholder="What are the slowest queries?")
    
    if st.button("🔍 Test Query"):
        if question:
            st.write(f"**You asked**: {question}")
            st.info("🚧 MCP integration will be added once basic deployment works")
        else:
            st.warning("Please enter a question")
    
    # Static demo data
    st.header("📊 Sample Analytics")
    
    import pandas as pd
    sample_data = {
        "Query Type": ["SELECT *", "Unbounded Sort", "Cartesian Join"],
        "Potential Savings": ["40%", "60%", "80%"], 
        "Monthly Cost": ["$1,200", "$800", "$2,000"]
    }
    
    df = pd.DataFrame(sample_data)
    st.dataframe(df, use_container_width=True)
    
    st.success("🎯 Basic app is working! Ready to add MCP features.")

if __name__ == "__main__":
    main()