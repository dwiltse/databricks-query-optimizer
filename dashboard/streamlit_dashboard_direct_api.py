import streamlit as st
import requests
import json
from databricks.sdk import WorkspaceClient

st.set_page_config(
    page_title="Query Optimization Dashboard",
    page_icon="🚀",
    layout="wide"
)

def main():
    st.title("🚀 Databricks Query Optimization Dashboard")
    st.markdown("**Direct Genie API Integration** (No MCP Required)")
    
    # Connection Status Section
    st.header("🔗 Connection Status")
    
    workspace_connected = False
    genie_connected = False
    
    try:
        # Connect to Databricks workspace
        workspace_client = WorkspaceClient()
        workspace_connected = True
        st.success("✅ Connected to Databricks workspace")
        st.write(f"**Host:** {workspace_client.config.host}")
        
        # Test Direct Genie API connection
        try:
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
            
            response = requests.get(list_spaces_url, headers=headers)
            if response.status_code == 200:
                spaces = response.json()
                st.success("✅ Direct Genie API connected!")
                st.write("**Available Genie Spaces:**")
                
                target_space_found = False
                for space in spaces.get('spaces', []):
                    display_name = space.get('display_name', 'Unknown')
                    space_id = space.get('id', 'Unknown')
                    st.write(f"- **{display_name}** (ID: `{space_id}`)")
                    
                    if space_id == genie_space_id:
                        st.info(f"🎯 Found target space: **{display_name}**")
                        target_space_found = True
                
                if target_space_found:
                    genie_connected = True
                    st.success("✅ Ready to use Direct Genie API")
                else:
                    st.warning(f"⚠️ Target space '{genie_space_id}' not found in available spaces")
                    genie_connected = False
            else:
                st.error(f"❌ API connection failed: {response.status_code}")
                st.write(f"Response: {response.text}")
                genie_connected = False
                
        except Exception as api_error:
            st.error(f"❌ Direct API connection failed: {api_error}")
            genie_connected = False
            
    except Exception as workspace_error:
        st.error(f"❌ Workspace connection failed: {workspace_error}")
        workspace_connected = False
    
    # Store connection status in session state
    if 'connections' not in st.session_state:
        st.session_state.connections = {}
    
    st.session_state.connections['workspace'] = workspace_connected
    st.session_state.connections['genie'] = genie_connected
    st.session_state.connections['workspace_client'] = workspace_client if workspace_connected else None
    st.session_state.connections['genie_space_id'] = genie_space_id if genie_connected else None
    
    # Query Interface Section
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
                        with st.spinner("🤖 Querying Genie Space via Direct API..."):
                            workspace_client = st.session_state.connections['workspace_client']
                            space_id = st.session_state.connections['genie_space_id']
                            
                            # Start a new conversation
                            start_conversation_url = f"{workspace_client.config.host}/api/2.0/genie/spaces/{space_id}/start-conversation"
                            
                            headers = {
                                "Authorization": f"Bearer {workspace_client.config.token}",
                                "Content-Type": "application/json"
                            }
                            
                            payload = {
                                "content": question
                            }
                            
                            st.write("**Debug Info:**")
                            st.write(f"Space ID: {space_id}")
                            st.write(f"API Endpoint: {start_conversation_url}")
                            st.write(f"Question: {question}")
                            
                            # Make the API call
                            response = requests.post(start_conversation_url, headers=headers, json=payload)
                            
                            if response.status_code == 200:
                                result = response.json()
                                st.success("✅ Direct Genie API Query completed!")
                                st.subheader("🤖 AI Analysis")
                                
                                # Display the response content
                                if 'content' in result:
                                    st.write(result['content'])
                                elif 'message' in result:
                                    st.write(result['message'])
                                elif 'messages' in result and len(result['messages']) > 0:
                                    # Handle conversation format
                                    for msg in result['messages']:
                                        if msg.get('role') == 'assistant':
                                            st.write(msg.get('content', ''))
                                else:
                                    st.write("**Full Response:**")
                                    st.json(result)
                                
                                st.balloons()
                            else:
                                st.error(f"❌ API request failed: {response.status_code}")
                                st.write("**Error Response:**")
                                st.code(response.text)
                                
                                # Show helpful error info
                                if response.status_code == 404:
                                    st.info("💡 The Genie space might not exist or you don't have access to it")
                                elif response.status_code == 401:
                                    st.info("💡 Authentication failed - check your token")
                                elif response.status_code == 403:
                                    st.info("💡 Access denied - check your permissions for the Genie space")
                    
                    except Exception as e:
                        st.error(f"❌ Direct API query failed: {str(e)}")
                        st.write("**Detailed Error Info**")
                        st.code(str(e))
                else:
                    st.warning("⚠️ Direct Genie API not connected - cannot query Genie Space")
                    st.info("💡 Make sure workspace and Genie space connections are successful above")
            else:
                st.warning("Please enter a question first")
    
    with col2:
        st.subheader("📊 Connection Summary")
        st.write(f"**Workspace:** {'✅' if workspace_connected else '❌'}")
        st.write(f"**Genie API:** {'✅' if genie_connected else '❌'}")
        
        if workspace_connected and genie_connected:
            st.success("🎉 All systems ready!")
        else:
            st.warning("⚠️ Some connections failed")
            
        st.subheader("💡 Sample Questions")
        st.write("- What are the slowest queries?")
        st.write("- Show me queries that use the most resources")
        st.write("- Which users have expensive queries?")
        st.write("- What are the most common query patterns?")

if __name__ == "__main__":
    main()