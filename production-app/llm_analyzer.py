"""
LLM Analysis Engine - The brain of our optimization beast!
This better work or dwiltse will never trust me again! 😅
"""

from openai import OpenAI
import os
import json
import streamlit as st
from databricks.sdk import WorkspaceClient

class LLMAnalysisEngine:
    """
    The AI brain that analyzes queries and generates optimizations
    Using Llama 70B for cost-effective intelligence!
    """
    
    def __init__(self, model="databricks-meta-llama-3-3-70b-instruct"):
        self.model = model
        self.client = self._initialize_client()
        self.workspace_client = WorkspaceClient()
    
    def _initialize_client(self):
        """Initialize OpenAI client for Databricks endpoints with consistent auth"""
        try:
            # Use workspace client for consistent authentication
            self.workspace_client = WorkspaceClient()
            
            # Get token from workspace client (most reliable in Databricks Apps)
            token = self.workspace_client.config.token
            base_url = f"{self.workspace_client.config.host}/serving-endpoints"
            
            if not token:
                raise ValueError("No authentication token available from workspace client")
            
            client = OpenAI(
                api_key=token,
                base_url=base_url
            )
            
            print(f"🤖 LLM client initialized with model: {self.model}")
            print(f"🔗 Base URL: {base_url}")
            return client
            
        except Exception as e:
            print(f"❌ Failed to initialize LLM client: {e}")
            return None
    
    def analyze_query(self, query_text, context_data=None):
        """
        Analyze a query and provide optimization recommendations
        This is where the magic happens! 🎩✨
        """
        
        if not self.client:
            return {"success": False, "error": "LLM client not initialized"}
        
        # Create the analysis prompt
        system_prompt = """You are an expert Databricks SQL optimization specialist with deep knowledge of Spark SQL performance tuning.

Analyze the provided SQL query and provide:

1. **Performance Issues Identified:**
   - Specific bottlenecks and inefficiencies
   - Root causes of slow performance
   
2. **Optimization Recommendations:**
   - Concrete, implementable improvements
   - Indexing strategies
   - Query rewriting suggestions
   - JOIN optimization opportunities
   - Partitioning strategies
   
3. **Expected Impact:**
   - Estimated performance improvement percentages
   - Expected cost savings
   - Implementation difficulty (Easy/Medium/Hard)

4. **Implementation Priority:**
   - Rank recommendations by impact vs effort
   - Quick wins vs long-term improvements

Be specific with column names, table references, and SQL syntax. Focus on actionable recommendations that a developer can implement immediately."""

        user_prompt = f"""
Query to analyze:
```sql
{query_text}
```

{f"Additional context: {context_data}" if context_data else ""}

Provide a comprehensive optimization analysis with specific, implementable recommendations.
"""

        try:
            print(f"🧠 Analyzing query with {self.model}...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,  # Low temperature for consistent technical analysis
                max_tokens=2000
            )
            
            analysis_result = response.choices[0].message.content
            
            return {
                "success": True,
                "analysis": analysis_result,
                "recommendations": self._extract_recommendations(analysis_result),
                "model_used": self.model
            }
            
        except Exception as e:
            return {"success": False, "error": f"LLM analysis failed: {str(e)}"}
    
    def generate_optimized_query(self, original_query, analysis_result):
        """
        Generate the actual optimized SQL query
        The moment of truth! 🎯
        """
        
        if not self.client:
            return {"success": False, "error": "LLM client not initialized"}
        
        system_prompt = """You are an expert SQL developer specializing in Databricks/Spark SQL optimization.

Given an original query and optimization analysis, generate:

1. **Optimized SQL Query:**
   - Rewritten query implementing the recommendations
   - Proper SQL formatting with clear comments
   - Maintain identical functionality while improving performance

2. **Key Changes Made:**
   - List specific optimizations applied
   - Explain why each change improves performance

3. **Implementation Notes:**
   - Any prerequisites (indexes, table changes needed)
   - Potential gotchas or considerations
   - Testing recommendations

Generate clean, production-ready SQL code that maintains the original query's logic while implementing performance improvements."""

        user_prompt = f"""
Original Query:
```sql
{original_query}
```

Optimization Analysis:
{analysis_result}

Generate an optimized version of this query implementing the recommended improvements. Include comments explaining the key optimizations made.
"""

        try:
            print(f"🛠️ Generating optimized query with {self.model}...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,  # Low temperature for consistent code generation
                max_tokens=2000
            )
            
            optimization_result = response.choices[0].message.content
            
            # Extract the optimized query from the response
            optimized_query = self._extract_sql_from_response(optimization_result)
            
            return {
                "success": True,
                "optimized_query": optimized_query,
                "full_response": optimization_result,
                "changes_made": self._extract_changes(optimization_result),
                "model_used": self.model
            }
            
        except Exception as e:
            return {"success": False, "error": f"Query optimization failed: {str(e)}"}
    
    def _extract_recommendations(self, analysis_text):
        """Extract key recommendations from analysis text"""
        # Simple extraction - look for numbered lists or bullet points
        recommendations = []
        lines = analysis_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if (line.startswith('- ') or 
                line.startswith('* ') or 
                any(line.startswith(f'{i}.') for i in range(1, 10))):
                recommendations.append(line)
        
        return recommendations[:5]  # Top 5 recommendations
    
    def _extract_sql_from_response(self, response_text):
        """Extract SQL code from LLM response"""
        # Look for SQL code blocks
        lines = response_text.split('\n')
        sql_lines = []
        in_sql_block = False
        
        for line in lines:
            if '```sql' in line.lower():
                in_sql_block = True
                continue
            elif '```' in line and in_sql_block:
                break
            elif in_sql_block:
                sql_lines.append(line)
        
        if sql_lines:
            return '\n'.join(sql_lines)
        else:
            # Fallback: return the full response if no code block found
            return response_text
    
    def _extract_changes(self, optimization_text):
        """Extract the key changes made from optimization response"""
        # Look for sections about changes or improvements
        changes = []
        lines = optimization_text.split('\n')
        
        in_changes_section = False
        for line in lines:
            line = line.strip()
            if any(keyword in line.lower() for keyword in ['changes made', 'optimizations', 'improvements']):
                in_changes_section = True
                continue
            elif in_changes_section and line:
                if line.startswith(('- ', '* ', '1.', '2.', '3.')):
                    changes.append(line)
                elif line.startswith('#') or line.startswith('**'):
                    break
        
        return changes[:3]  # Top 3 changes
    
    def switch_to_claude_sonnet(self):
        """
        Upgrade to Claude Sonnet 3.7 for premium code generation
        The premium upgrade! 🚀
        """
        self.model = "databricks-claude-3-7-sonnet"
        print("🚀 UPGRADED TO CLAUDE SONNET 3.7! Premium code generation activated!")
        return {"status": "upgraded", "model": self.model}
    
    def test_llm_connection(self):
        """Test if the LLM connection is working"""
        if not self.client:
            return {"status": "error", "message": "Client not initialized"}
        
        try:
            # Simple test query
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": "Say 'LLM connection working!' if you can read this."}
                ],
                max_tokens=50
            )
            
            result = response.choices[0].message.content
            
            return {
                "status": "success", 
                "message": f"✅ {self.model} connected!",
                "test_response": result
            }
            
        except Exception as e:
            return {"status": "error", "message": f"Connection test failed: {str(e)}"}

# Streamlit integration helpers
@st.cache_resource
def get_llm_analyzer():
    """Get LLM analyzer with caching for Streamlit"""
    return LLMAnalysisEngine()

def display_llm_status():
    """Display LLM connection status in Streamlit"""
    llm = get_llm_analyzer()
    status = llm.test_llm_connection()
    
    if status["status"] == "success":
        st.success(f"✅ {status['message']}")
        st.info(f"**Current Model:** {llm.model}")
        
        # Model upgrade option
        if "llama" in llm.model.lower():
            if st.button("🚀 Upgrade to Claude Sonnet 3.7"):
                upgrade_status = llm.switch_to_claude_sonnet()
                st.success(f"🚀 Upgraded to {upgrade_status['model']}!")
                st.rerun()
    else:
        st.error(f"❌ LLM Connection Failed: {status['message']}")
        st.write("**Troubleshooting:**")
        st.write("- Check that your Llama 70B endpoint is available")
        st.write("- Verify authentication token is valid")
        st.write("- Ensure serving endpoint is enabled")
    
    return status["status"] == "success"

# Test function for development
def test_llm_analyzer():
    """Test the LLM analyzer - crossing fingers! 🤞"""
    print("🧪 Testing LLM Analyzer...")
    
    llm = LLMAnalysisEngine()
    
    # Test 1: Connection
    status = llm.test_llm_connection()
    print(f"Connection Status: {status}")
    
    if status["status"] != "success":
        print("❌ LLM connection failed!")
        return False
    
    # Test 2: Query analysis
    print("\n🧠 Testing query analysis...")
    test_query = """
    SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count, SUM(o.total_amount) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.created_date >= '2023-01-01'
    GROUP BY c.customer_id, c.name
    ORDER BY total_spent DESC;
    """
    
    analysis = llm.analyze_query(test_query)
    print(f"Analysis Result: {analysis.get('success', False)}")
    if analysis.get('success'):
        print("✅ Query analysis working!")
    else:
        print(f"❌ Analysis failed: {analysis.get('error')}")
        return False
    
    # Test 3: Query optimization
    print("\n🛠️ Testing query optimization...")
    optimization = llm.generate_optimized_query(test_query, analysis['analysis'])
    print(f"Optimization Result: {optimization.get('success', False)}")
    if optimization.get('success'):
        print("✅ Query optimization working!")
        print(f"Optimized Query Preview: {optimization['optimized_query'][:100]}...")
    else:
        print(f"❌ Optimization failed: {optimization.get('error')}")
        return False
    
    print("\n🎉 LLM Analyzer Test Complete!")
    return True

if __name__ == "__main__":
    # Run the test
    test_llm_analyzer()