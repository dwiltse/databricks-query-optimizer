# Query Optimization Production App Architecture

## 🎯 **App Overview**
A Databricks App that automatically identifies poor-performing queries, analyzes them using MCP + Genie space, and generates optimized SQL scripts with LLM intelligence.

## 🏗️ **Technical Stack**

### **Core Components:**
- **Platform**: Databricks Apps (secure, internal)
- **Data Access**: Managed MCP → Genie Space → System Tables
- **LLM Strategy**: Llama 70B (MVP) → Claude Sonnet 3.7 (Production)
- **Frontend**: Streamlit (familiar, fast development)
- **Authentication**: Workspace-based (no tokens needed)

### **Data Flow:**
```
System Tables → Genie Space → MCP Connection → 
Databricks App → LLM Analysis → Optimization Scripts → Export
```

## 📊 **App Interface Design**

### **Main Dashboard:**
```
🚀 Query Optimization Assistant
┌─────────────────────────────────────────────┐
│ 📈 Performance Overview                     │
│ • Total queries analyzed: 1,247            │
│ • Optimization opportunities: 23           │
│ • Potential savings: $2,400/month          │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ 🔍 Query Analysis Options                   │
│                                             │
│ Time Range: [Last 24 hours ▼]              │
│ Min Duration: [>5 seconds    ]              │
│ Min Cost: [>$10 DBUs       ]                │
│                                             │
│ [🔍 Find Optimization Opportunities]        │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│ 🐌 Worst Performing Queries                │
│                                             │
│ 1. Customer Analytics (8.2 min, $45 DBUs)  │
│    [📊 Analyze] [🛠️ Optimize]               │
│                                             │
│ 2. Sales Report (6.1 min, $32 DBUs)        │  
│    [📊 Analyze] [🛠️ Optimize]               │
│                                             │
│ 3. Inventory Scan (4.8 min, $28 DBUs)      │
│    [📊 Analyze] [🛠️ Optimize]               │
└─────────────────────────────────────────────┘
```

### **Optimization Results Page:**
```
🛠️ Query Optimization Results

Original Query Performance:
• Execution Time: 8.2 minutes
• DBU Cost: $45
• Rows Scanned: 2.1B
• Data Volume: 850 GB

🤖 LLM Analysis:
"This query performs full table scans on orders and customers 
tables with inefficient JOIN conditions. The main bottlenecks are..."

💡 Recommended Optimizations:
1. Add composite index on (customer_id, order_date)
2. Replace subquery with materialized view  
3. Partition orders table by month
4. Use BROADCAST hint for customers table

📈 Expected Improvements:
• Execution Time: 2.1 minutes (74% faster)
• DBU Cost: $12 (73% savings)
• Annual Savings: $8,400

📄 Generated Scripts:
[📋 Copy Optimized Query] [⬇️ Download Test Script]
```

## 🔧 **Core Components**

### **1. Query Detection Engine**
```python
class QueryDetectionEngine:
    """Identifies queries that need optimization"""
    
    def __init__(self, mcp_client, criteria):
        self.mcp_client = mcp_client
        self.criteria = criteria
    
    def find_worst_queries(self, hours_back=24, limit=10):
        """Use MCP to query Genie space for worst performers"""
        query_request = f"""
        Find the {limit} worst performing queries in the last {hours_back} hours
        that have execution time > {self.criteria['min_duration']} seconds
        and DBU cost > {self.criteria['min_cost']}
        Include: query_id, statement_text, execution_time, dbu_cost, frequency
        """
        
        response = self.mcp_client.call_tool("query", {"question": query_request})
        return self.parse_query_results(response)
    
    def get_query_context(self, query_id):
        """Get detailed context for a specific query"""
        context_request = f"""
        For query_id {query_id}, provide:
        - Full execution plan
        - Table scan statistics  
        - JOIN patterns and performance
        - Historical performance trends
        - Resource utilization breakdown
        """
        
        return self.mcp_client.call_tool("query", {"question": context_request})
```

### **2. LLM Analysis Engine** 
```python
class LLMAnalysisEngine:
    """Analyzes queries using Llama 70B or Claude Sonnet 3.7"""
    
    def __init__(self, model_endpoint="databricks-meta-llama-3-3-70b-instruct"):
        self.model_endpoint = model_endpoint
        self.client = self._initialize_client()
    
    def analyze_query_performance(self, query_data, context_data):
        """Generate optimization recommendations"""
        
        system_prompt = """You are an expert Databricks query optimization specialist.
        
        Analyze the provided query performance data and generate:
        1. Root cause analysis of performance bottlenecks
        2. Specific, implementable optimization recommendations
        3. Expected impact estimates (time and cost savings)
        4. Implementation priority ranking
        
        Focus on:
        - Indexing strategies
        - Query rewriting opportunities  
        - Partitioning improvements
        - JOIN optimization
        - Materialized view candidates
        
        Be specific with column names, table structures, and SQL syntax."""
        
        user_prompt = f"""
        Query Performance Data:
        {query_data}
        
        Execution Context:
        {context_data}
        
        Provide detailed optimization analysis with specific recommendations.
        """
        
        response = self.client.chat.completions.create(
            model=self.model_endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1  # Low temperature for consistent technical analysis
        )
        
        return self.parse_optimization_recommendations(response)
    
    def generate_optimized_query(self, original_query, recommendations):
        """Generate the actual optimized SQL code"""
        
        code_prompt = f"""
        Original Query:
        {original_query}
        
        Optimization Recommendations:
        {recommendations}
        
        Generate:
        1. Optimized version of the SQL query
        2. DDL statements for recommended indexes/partitions
        3. Test script to compare performance (A/B test)
        4. Rollback script if needed
        
        Use proper SQL formatting and include comments explaining changes.
        """
        
        response = self.client.chat.completions.create(
            model=self.model_endpoint,
            messages=[
                {"role": "system", "content": "You are an expert SQL developer. Generate clean, optimized, well-commented SQL code."},
                {"role": "user", "content": code_prompt}
            ],
            temperature=0.1
        )
        
        return self.parse_generated_code(response)
    
    def switch_to_claude_sonnet(self):
        """Upgrade to Claude Sonnet 3.7 for better code generation"""
        self.model_endpoint = "databricks-claude-3-7-sonnet"
        self.client = self._initialize_client()
        print("🚀 Upgraded to Claude Sonnet 3.7 for premium code generation!")
```

### **3. MCP Connection Manager**
```python
class MCPConnectionManager:
    """Manages connection to Genie space via MCP"""
    
    def __init__(self, genie_space_id="system_table_mcp_test"):
        self.workspace_client = WorkspaceClient()
        self.genie_space_id = genie_space_id
        self.mcp_client = self._initialize_mcp_client()
    
    def _initialize_mcp_client(self):
        """Initialize MCP client with proven working pattern"""
        mcp_url = f"{self.workspace_client.config.host}/api/2.0/mcp/genie/{self.genie_space_id}"
        
        return DatabricksMCPClient(
            server_url=mcp_url,
            workspace_client=self.workspace_client
        )
    
    def test_connection(self):
        """Verify MCP connection is working"""
        try:
            tools = self.mcp_client.list_tools()
            return {"status": "success", "tools_available": len(tools)}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def query_system_tables(self, question):
        """Query system tables via Genie space"""
        return self.mcp_client.call_tool("query", {"question": question})
```

### **4. Results Export Manager**
```python
class ResultsExportManager:
    """Handles exporting optimization scripts and results"""
    
    def export_optimization_package(self, query_id, optimization_results):
        """Create downloadable package with all optimization materials"""
        
        package = {
            "query_id": query_id,
            "timestamp": datetime.now().isoformat(),
            "files": {
                "original_query.sql": optimization_results["original_query"],
                "optimized_query.sql": optimization_results["optimized_query"],
                "performance_test.sql": optimization_results["test_script"],
                "ddl_changes.sql": optimization_results["ddl_statements"],
                "analysis_report.md": optimization_results["analysis_report"],
                "rollback_script.sql": optimization_results["rollback_script"]
            }
        }
        
        return self.create_zip_download(package)
    
    def generate_test_script(self, original_query, optimized_query):
        """Generate A/B testing script"""
        
        test_script = f"""
-- Query Optimization A/B Test Script
-- Generated: {datetime.now().isoformat()}

-- Step 1: Create test tables (if needed)
-- {self.generate_test_setup()}

-- Step 2: Test original query performance
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;

-- Original Query Test
SELECT 'ORIGINAL' as test_type, current_timestamp() as start_time;
{original_query};
SELECT 'ORIGINAL' as test_type, current_timestamp() as end_time;

-- Step 3: Apply optimizations
{optimized_query}

-- Step 4: Test optimized query performance  
SELECT 'OPTIMIZED' as test_type, current_timestamp() as start_time;
{optimized_query};
SELECT 'OPTIMIZED' as test_type, current_timestamp() as end_time;

-- Step 5: Compare results
-- Check query history for performance comparison
SELECT 
    statement_text,
    execution_duration_ms,
    total_duration_ms,
    rows_read,
    bytes_read
FROM system.query.history 
WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
    AND (statement_text LIKE '%ORIGINAL%' OR statement_text LIKE '%OPTIMIZED%')
ORDER BY start_time DESC;
"""
        return test_script
```

## 🚀 **Deployment Architecture**

### **Databricks App Structure:**
```
query-optimization-app/
├── app.py                 # Main Streamlit app
├── components/
│   ├── query_detector.py  # Query detection logic
│   ├── llm_analyzer.py    # LLM analysis engine  
│   ├── mcp_manager.py     # MCP connection management
│   └── export_manager.py  # Results export functionality
├── config/
│   ├── app.yaml          # Databricks app configuration
│   └── requirements.txt   # Python dependencies
└── templates/
    ├── optimization_prompts.py  # LLM prompt templates
    └── sql_templates.py         # SQL script templates
```

### **Configuration (app.yaml):**
```yaml
command:
  - "streamlit"
  - "run" 
  - "app.py"
  - "--server.port=8000"

env:
  - name: "DATABRICKS_APP_PORT"
    value: "8000"
  - name: "GENIE_SPACE_ID"
    value: "system_table_mcp_test"

runtime: python_3.11
```

### **Dependencies (requirements.txt):**
```
streamlit>=1.28.0
databricks-mcp>=1.0.0
databricks-sdk>=0.18.0
databricks-agents>=1.0.0
pandas>=2.0.0
plotly>=5.17.0
python-dotenv>=1.0.0
```

## 🎯 **Deployment Strategy**

### **Phase 1: MVP with Llama 70B** (2-3 weeks)
- ✅ Core functionality with cost-effective LLM
- ✅ Validate user workflow and feedback
- ✅ Refine optimization recommendations
- ✅ Test MCP connection stability

### **Phase 2: Upgrade to Claude Sonnet 3.7** (1 week)
- 🚀 Switch LLM endpoint for better code generation
- 🚀 Enhanced SQL optimization quality
- 🚀 More sophisticated analysis
- 🚀 Better edge case handling

### **Phase 3: Advanced Features** (Future)
- 📈 Automated optimization scheduling
- 📊 Performance trend analysis  
- 🔄 Integration with CI/CD pipelines
- 🏆 ROI tracking and reporting

## 💡 **Key Advantages**

### **Technical:**
- **Proven MCP Connection**: Uses your validated playground setup
- **Cost-Effective Start**: Llama 70B for development/validation
- **Premium Upgrade Path**: Easy switch to Claude Sonnet 3.7
- **Secure Architecture**: Everything stays within Databricks

### **Business:**
- **Immediate Value**: Identifies optimization opportunities automatically
- **Measurable ROI**: Tracks cost savings and performance improvements  
- **User-Friendly**: Simple interface for non-technical users
- **Scalable**: Can process hundreds of queries automatically

### **Development:**
- **Rapid Prototyping**: Streamlit for fast iteration
- **Modular Design**: Easy to enhance and maintain
- **Battle-Tested Components**: Uses proven Databricks patterns

## 🎉 **Success Metrics**

- **Performance**: Average query optimization reduces execution time by 40%+
- **Cost**: Monthly DBU savings of $10,000+ identified  
- **Adoption**: 80% of optimization recommendations implemented
- **Quality**: 90% of generated SQL scripts work without modification

This architecture gives you the **perfect evolution path**: Start cost-effective with Llama 70B, prove the value, then upgrade to Claude Sonnet 3.7 for premium code generation! 🚀