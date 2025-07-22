# Databricks Apps Deployment Guide

## Step-by-Step Deployment Process

### 1. Upload to Databricks Workspace

**Location:** Your work Databricks workspace

1. **Create Folder:**
   - Navigate to Workspace → Create → Folder
   - Name: `query-optimization-dashboard`

2. **Upload Files:**
   ```
   📁 query-optimization-dashboard/
   ├── 📄 query_optimization_dashboard.py
   ├── 📄 requirements.txt
   ├── 📄 README.md
   └── 📄 DEPLOYMENT.md (this file)
   ```

### 2. Create Databricks App

**Location:** Databricks Workspace → Apps

1. **Navigate to Apps:**
   - Left sidebar → "Apps" 
   - Click "Create App"

2. **App Configuration:**
   ```yaml
   App Name: Query Optimization Dashboard
   Description: MCP-powered query optimization insights
   Source Code: /Workspace/Users/your-email/query-optimization-dashboard/
   Entry Point: query_optimization_dashboard.py
   Compute Size: Small (can upgrade to Medium if needed)
   ```

3. **Environment Setup:**
   - Python Version: 3.10 or 3.11
   - Dependencies: Will auto-install from requirements.txt

### 3. Prerequisites Check

**Before deploying, ensure:**

✅ **Genie Space Exists:**
- Genie Space named `system_table_mcp_test` is created
- Contains the 4 tables: query_performance_raw, query_patterns, optimization_tracking, performance_baselines

✅ **MCP Access:**
- Managed MCP is enabled in your workspace
- Your user has permissions to access Genie Spaces

✅ **Data Population:**
- Tables have been populated with data using your SQL scripts
- At least some query performance data exists for meaningful results

### 4. Deploy and Test

1. **Deploy App:**
   - Click "Deploy" in Databricks Apps
   - Wait for build to complete (installs requirements.txt)

2. **Access App:**
   - Gets URL like: `https://your-workspace.cloud.databricks.com/apps/your-app-id`
   - Share this URL with stakeholders for demo

3. **Test Functionality:**
   - Connection status should show "✅ Connected to Databricks MCP"
   - Try pre-defined questions from dropdown
   - Verify Genie Space queries return data

## Key Differences from Local Development

| Aspect | Local Development | Databricks Apps |
|--------|------------------|-----------------|
| **Authentication** | Manual CLI setup | Automatic |
| **MCP Connection** | External connection | Built-in workspace access |
| **Genie Space Access** | Cross-workspace auth | Same workspace, seamless |
| **Deployment** | `streamlit run` | Deploy button |
| **URL Access** | localhost:8501 | Workspace-hosted URL |
| **Permissions** | Personal token | Workspace identity |

## Troubleshooting

### Connection Issues
**Error:** "Could not connect to Databricks MCP"
**Solution:** 
- Verify MCP is enabled in workspace settings
- Check user permissions for Genie Space access
- Ensure app is deployed in correct workspace

### Query Issues  
**Error:** "Genie Space query failed"
**Solution:**
- Confirm `system_table_mcp_test` Genie Space exists
- Verify tables contain data (run population SQL scripts)
- Check table names match exactly

### App Deployment Issues
**Error:** "Build failed"
**Solution:**
- Check requirements.txt syntax
- Verify Python version compatibility
- Review Databricks Apps logs for specific errors

## Demo Script

Once deployed, use this script for demos:

1. **Show Connection Status:**
   "The app automatically connects to our Databricks workspace and accesses the query optimization Genie Space securely."

2. **Natural Language Query:**
   "Let me ask: 'What are the top 3 slowest queries this week?' using plain English - no SQL required."

3. **Show Results:**
   "The AI analyzes our actual query performance data and provides actionable insights with cost impact."

4. **Highlight Value:**
   "Business users can now get query optimization insights without knowing SQL, while maintaining full Databricks security."

## Production Considerations

- **Scaling:** Upgrade compute size if handling large result sets
- **Permissions:** Set up proper access controls for different user roles
- **Monitoring:** Use Databricks Apps monitoring for usage analytics
- **Updates:** Redeploy app when updating dashboard features