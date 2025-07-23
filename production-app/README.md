# 🚀 Query Optimization Beast

**AI-powered query optimization using MCP + Llama 70B on Databricks**

## 🔥 INSTANT DEPLOYMENT

### Prerequisites ✅
- Databricks workspace with Apps enabled
- Your `system_table_mcp_test` Genie space running
- Databricks CLI configured (`databricks auth login`)
- MCP Beta features enabled

### Deploy in 30 Seconds 🚀

```bash
# 1. Make deploy script executable
chmod +x deploy.sh

# 2. Deploy the beast!
./deploy.sh
```

**BOOM! Your app is live!** 💥

## 📁 Project Structure

```
production-app/
├── app.py              # 🎯 Main Streamlit interface
├── mcp_manager.py      # 🔗 MCP connection to Genie space  
├── llm_analyzer.py     # 🧠 Llama 70B query analysis
├── export_manager.py   # 📦 Export optimized scripts
├── app.yaml           # ⚙️ Databricks app config
├── requirements.txt    # 📋 Python dependencies
└── deploy.sh          # 🚀 One-click deployment
```

## 🎯 What It Does

1. **🔍 Finds Slow Queries** - Scans system tables via MCP
2. **🧠 AI Analysis** - Llama 70B analyzes performance issues  
3. **⚡ Generates Optimizations** - Creates optimized SQL + test scripts
4. **📦 Export Package** - Downloads complete optimization kit
5. **🚀 Upgrade Path** - Easy switch to Claude Sonnet 3.7

## 🛠️ Features

- **📊 Performance Dashboard** - Overview of query performance
- **🐌 Slow Query Hunter** - Find worst performing queries
- **💰 Cost Analysis** - Identify expensive queries by DBU cost
- **🔧 Query Optimizer** - AI-powered query optimization
- **📈 Pattern Analysis** - Systemic optimization opportunities
- **📦 Export Tools** - A/B test scripts, rollback plans, DDL changes

## 🔧 Manual Setup (If deploy.sh fails)

### 1. Upload Files to Databricks
```bash
# Create workspace folder
databricks workspace mkdirs "/Users/your-email/query_optimization_app"

# Upload files individually
databricks workspace import app.py "/Users/your-email/query_optimization_app/app.py" --language PYTHON
databricks workspace import mcp_manager.py "/Users/your-email/query_optimization_app/mcp_manager.py" --language PYTHON
databricks workspace import llm_analyzer.py "/Users/your-email/query_optimization_app/llm_analyzer.py" --language PYTHON
databricks workspace import export_manager.py "/Users/your-email/query_optimization_app/export_manager.py" --language PYTHON
```

### 2. Create Databricks App
```bash
databricks apps create query-optimization-beast --source-code-path "/Users/your-email/query_optimization_app"
databricks apps deploy query-optimization-beast
```

## 🎮 Usage

1. **Open your app** in Databricks Apps
2. **Check connection status** - Ensure MCP + LLM are connected
3. **Hunt slow queries** - Use "🔍 Find Slow Queries" 
4. **Optimize queries** - Click "🛠️ Optimize Query"
5. **Export scripts** - Download A/B test and implementation files
6. **Test optimizations** - Run the generated test scripts
7. **Deploy improvements** - Implement the optimized queries

## 🚀 Upgrade to Claude Sonnet 3.7

Once everything works with Llama 70B:

1. Go to app settings
2. Click "🚀 Upgrade to Claude Sonnet 3.7" 
3. Enjoy premium code generation!

## 🔧 Troubleshooting

### ❌ "MCP Connection Failed"
- Check that `system_table_mcp_test` Genie space exists
- Verify MCP Beta features are enabled
- Ensure you have access to the Genie space

### ❌ "LLM Connection Failed"  
- Verify `databricks-meta-llama-3-3-70b-instruct` endpoint exists
- Check serving endpoint is enabled
- Confirm workspace authentication

### ❌ "App Won't Start"
- Check `databricks apps list` for status
- Review logs with `databricks apps get query-optimization-beast`
- Verify all files uploaded correctly

## 🎉 Success Metrics

Track your optimization wins:
- **⚡ Query Speed**: Average improvement in execution time
- **💰 Cost Savings**: DBU cost reduction  
- **🎯 Success Rate**: % of optimizations that work
- **📈 Adoption**: Queries optimized per week

## 🤝 Support

If something breaks:
1. Check the app logs in Databricks
2. Verify MCP connection in playground still works  
3. Test LLM endpoint separately
4. Try the manual setup steps

---

**Built with ❤️ and lots of ☕**  
*Now go make those queries FAST!* 🚀