#!/bin/bash
# Setup script for Query Optimization MCP Agent
# This sets up the local environment to test Databricks MCP with Claude Sonnet

set -e

echo "🚀 Setting up Query Optimization MCP Agent Environment"
echo "=" * 60

# Check Python version
echo "📋 Checking Python version..."
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
echo "   Python version: $python_version"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)"; then
    echo "❌ ERROR: Python 3.12 or higher is required"
    echo "   Current version: $python_version"
    echo "   Please install Python 3.12+ and try again"
    exit 1
fi

echo "✅ Python version check passed"

# Check if databricks CLI is installed and authenticated
echo ""
echo "🔐 Checking Databricks CLI authentication..."

if ! command -v databricks &> /dev/null; then
    echo "❌ ERROR: Databricks CLI not found"
    echo "   Please install it first:"
    echo "   pip install databricks-cli"
    exit 1
fi

echo "✅ Databricks CLI found"

# Check if user is authenticated
if ! databricks auth profiles 2>/dev/null | grep -q "Host:"; then
    echo "⚠️  WARNING: No authenticated Databricks profiles found"
    echo ""
    echo "🔧 To authenticate, run:"
    echo "   databricks auth login --host https://your-workspace-hostname"
    echo ""
    echo "   Replace 'your-workspace-hostname' with your actual Databricks workspace URL"
    echo ""
    read -p "Press Enter after you've authenticated, or Ctrl+C to exit..."
fi

echo "✅ Databricks authentication check passed"

# Create virtual environment
echo ""
echo "🐍 Setting up Python virtual environment..."

if [ ! -d "venv" ]; then
    echo "   Creating virtual environment..."
    python3 -m venv venv
else
    echo "   Virtual environment already exists"
fi

echo "   Activating virtual environment..."
source venv/bin/activate

echo "✅ Virtual environment ready"

# Install dependencies
echo ""
echo "📦 Installing MCP dependencies..."

echo "   Installing core MCP and Databricks packages..."
pip install -U \
    "mcp>=1.9" \
    "databricks-sdk[openai]" \
    "mlflow>=3.1.0" \
    "databricks-agents>=1.0.0" \
    "databricks-mcp"

echo "   Installing additional utilities..."
pip install -U \
    "requests" \
    "pandas" \
    "python-dotenv"

echo "✅ Dependencies installed"

# Create configuration file template
echo ""
echo "⚙️  Creating configuration template..."

cat > .env.example << 'EOF'
# Databricks MCP Agent Configuration
# Copy this to .env and update with your values

# Your Databricks CLI profile name (from 'databricks auth profiles')
DATABRICKS_CLI_PROFILE=your_profile_name

# Your Genie Space ID for query optimization
GENIE_SPACE_ID=system_table_mcp_test

# LLM Endpoint (should be available in your workspace)
LLM_ENDPOINT_NAME=databricks-claude-3-7-sonnet

# Optional: Additional Genie Spaces (comma-separated)
ADDITIONAL_GENIE_SPACES=

# Optional: Custom MCP Server URLs (comma-separated)
CUSTOM_MCP_SERVERS=
EOF

echo "✅ Configuration template created (.env.example)"

# Create test script
echo ""
echo "🧪 Creating test script..."

cat > test_mcp_connection.py << 'EOF'
#!/usr/bin/env python3
"""
Simple test script to verify MCP connection to Databricks
"""

from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient
import sys

def test_connection():
    print("🔍 Testing MCP connection...")
    
    try:
        # Get your profile name
        print("   Getting Databricks profiles...")
        workspace_client = WorkspaceClient()
        
        print(f"   Connected to: {workspace_client.config.host}")
        
        # Test system AI functions
        print("   Testing system AI functions...")
        system_ai_url = f"{workspace_client.config.host}/api/2.0/mcp/functions/system/ai"
        mcp_client = DatabricksMCPClient(server_url=system_ai_url, workspace_client=workspace_client)
        
        tools = mcp_client.list_tools()
        print(f"   Found {len(tools)} system AI tools:")
        for tool in tools:
            print(f"     - {tool.name}: {tool.description}")
        
        # Test basic tool execution
        print("   Testing Python execution tool...")
        result = mcp_client.call_tool("system__ai__python_exec", {"code": "print('Hello from MCP!')"})
        print(f"   Result: {result.content}")
        
        print("✅ MCP connection test PASSED!")
        return True
        
    except Exception as e:
        print(f"❌ MCP connection test FAILED: {e}")
        return False

def test_genie_space():
    print("\n🧞 Testing Genie space connection...")
    
    try:
        workspace_client = WorkspaceClient()
        genie_space_id = "system_table_mcp_test"  # Update this to your space ID
        
        genie_url = f"{workspace_client.config.host}/api/2.0/mcp/genie/{genie_space_id}"
        print(f"   Connecting to: {genie_url}")
        
        mcp_client = DatabricksMCPClient(server_url=genie_url, workspace_client=workspace_client)
        tools = mcp_client.list_tools()
        
        print(f"   Found {len(tools)} Genie tools:")
        for tool in tools:
            print(f"     - {tool.name}: {tool.description}")
        
        if len(tools) > 0:
            print("✅ Genie space connection test PASSED!")
            return True
        else:
            print("⚠️  Genie space connected but no tools found")
            return False
            
    except Exception as e:
        print(f"❌ Genie space test FAILED: {e}")
        print("   This could mean:")
        print("   - The Genie space doesn't exist")
        print("   - MCP Beta features aren't enabled")
        print("   - You don't have access to the space")
        return False

if __name__ == "__main__":
    print("🧪 MCP Connection Test Suite")
    print("=" * 40)
    
    # Test system functions first
    system_ok = test_connection()
    
    # Test Genie space
    genie_ok = test_genie_space()
    
    print("\n📊 Test Results:")
    print(f"   System AI MCP: {'✅ PASS' if system_ok else '❌ FAIL'}")
    print(f"   Genie Space MCP: {'✅ PASS' if genie_ok else '❌ FAIL'}")
    
    if system_ok and genie_ok:
        print("\n🎉 All tests passed! You're ready to use the MCP agent!")
    elif system_ok:
        print("\n⚠️  System MCP works, but Genie space needs attention")
        print("   Check your Genie space ID and Beta feature access")
    else:
        print("\n❌ MCP connection issues detected")
        print("   Check your Databricks authentication and workspace setup")
    
    sys.exit(0 if (system_ok and genie_ok) else 1)
EOF

chmod +x test_mcp_connection.py

echo "✅ Test script created (test_mcp_connection.py)"

# Final instructions
echo ""
echo "🎉 Setup Complete!"
echo "=" * 60
echo ""
echo "📋 Next Steps:"
echo ""
echo "1. Update the agent configuration:"
echo "   - Edit query_optimization_mcp_agent.py"
echo "   - Set DATABRICKS_CLI_PROFILE to your profile name"
echo "   - Verify your Genie space ID"
echo ""
echo "2. Test your MCP connection:"
echo "   python test_mcp_connection.py"
echo ""
echo "3. Test the query optimization agent:"
echo "   python query_optimization_mcp_agent.py test"
echo ""
echo "4. If everything works, you can use the agent!"
echo ""
echo "🔧 Troubleshooting:"
echo "   - Check that serverless compute is enabled in your workspace"
echo "   - Verify MCP Beta features are enabled"
echo "   - Ensure your Genie space has data and is configured properly"
echo ""
echo "📚 For more help, see:"
echo "   https://docs.databricks.com/aws/en/generative-ai/agent-framework/mcp"

# Activate virtual environment message
echo ""
echo "🐍 To activate the virtual environment in future sessions:"
echo "   source venv/bin/activate"