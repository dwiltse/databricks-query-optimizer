"""
Ultra-minimal Streamlit app for Databricks Apps
No advanced features, just basic Streamlit functionality
"""

import streamlit as st

# Basic page content
st.write("# 🚀 Query Optimization Dashboard")
st.write("**Status:** Testing basic Streamlit deployment")

# Simple text
st.write("✅ If you see this, Streamlit is working in Databricks Apps!")

# Basic input without session state
user_input = st.text_input("Enter a test message:")

if user_input:
    st.write(f"You entered: {user_input}")

# Simple button
if st.button("Test Button"):
    st.balloons()
    st.success("✅ Button clicked successfully!")

st.write("---")
st.write("🎯 **Next Steps:** Add MCP integration once this works")