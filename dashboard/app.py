#!/usr/bin/env python3
"""
Query Optimization Dashboard - Databricks Apps Entry Point
Based on working system_table_chain_of_debates pattern
"""

import subprocess
import sys
import os
import time

def main():
    """Launch the Streamlit application using the working pattern."""
    print("=== Starting Query Optimization Dashboard ===")
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Change to the project directory
    project_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_dir)
    print(f"Working directory: {project_dir}")
    
    # Print environment info for debugging
    print("Environment variables:")
    env_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_ACCESS_TOKEN', 'DATABRICKS_AUTH_TOKEN']
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            # Mask sensitive values
            masked = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
            print(f"  {var}: {masked}")
        else:
            print(f"  {var}: Not set")
    
    # Verify the Streamlit app exists
    streamlit_path = os.path.join(project_dir, 'streamlit_dashboard.py')
    if not os.path.exists(streamlit_path):
        print(f"ERROR: Streamlit app not found at {streamlit_path}")
        sys.exit(1)
    
    print(f"Found Streamlit app: {streamlit_path}")
    
    # Launch Streamlit using the working pattern - let Databricks Apps handle networking
    cmd = [
        sys.executable, '-m', 'streamlit', 'run', 
        'streamlit_dashboard.py',
        '--server.headless=true'
    ]
    
    print(f"Executing: {' '.join(cmd)}")
    print("=== Launching Streamlit ===")
    
    try:
        # Use exec to replace the current process - this works in Databricks Apps
        os.execvp(sys.executable, cmd)
    except Exception as e:
        print(f"ERROR launching Streamlit: {e}")
        print("Trying fallback method...")
        
        # Fallback to subprocess
        try:
            subprocess.run(cmd, check=True)
        except Exception as e2:
            print(f"ERROR with fallback: {e2}")
            sys.exit(1)

if __name__ == "__main__":
    main()