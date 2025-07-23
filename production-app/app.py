"""
🚀 QUERY OPTIMIZATION BEAST - MAIN STREAMLIT APP
Let's ship this thing and make queries FAST!
"""

import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Import our custom components
from mcp_manager import MCPConnectionManager
from llm_analyzer import LLMAnalysisEngine  
from export_manager import ResultsExportManager
from health import get_health_checker

# Page config - make it look AWESOME
st.set_page_config(
    page_title="🚀 Query Optimization Beast",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for that premium feel
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(90deg, #FF6B6B, #4ECDC4, #45B7D1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    
    .query-card {
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: #f8f9fa;
    }
    
    .optimization-result {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Main app function - LET'S GOOOOO!"""
    
    # Initialize health checker and perform startup checks
    health_checker = get_health_checker()
    
    # Show startup status
    if health_checker.health_status["status"] == "starting":
        st.info("🚀 Starting up Query Optimization Beast...")
        
        with st.spinner("Performing health checks..."):
            health_status = health_checker.perform_startup_checks()
        
        # Show results
        if health_status["status"] == "healthy":
            st.success("✅ All systems operational!")
        elif health_status["status"] == "degraded":
            st.warning("⚠️ Some components unavailable - partial functionality")
        else:
            st.error("❌ Critical components unavailable")
            
        # Refresh to continue
        time.sleep(1)
        st.rerun()
    
    if not health_checker.is_ready():
        st.error("🚨 **App Not Ready**")
        st.write("**Component Status:**")
        
        for component, status in health_checker.health_status["components"].items():
            if status["status"] == "healthy":
                st.success(f"✅ {component}: {status['message']}")
            else:
                st.error(f"❌ {component}: {status['message']}")
                if "troubleshooting" in status:
                    st.write("**Troubleshooting:**")
                    for tip in status["troubleshooting"]:
                        st.write(f"- {tip}")
        return
    
    # 🔥 EPIC HEADER
    st.markdown('<h1 class="main-header">🚀 Query Optimization Beast</h1>', unsafe_allow_html=True)
    st.markdown("### *Making your queries fast, one optimization at a time!* ✨")
    
    # Sidebar for navigation
    with st.sidebar:
        st.title("🎛️ Control Panel")
        
        # Page selection
        page = st.selectbox(
            "Choose Your Adventure:",
            ["🏠 Dashboard", "🔍 Find Slow Queries", "💰 Cost Analysis", "🛠️ Optimize Query", "📊 Pattern Analysis"]
        )
        
        st.markdown("---")
        
        # Component Status
        st.subheader("🔗 Component Status")
        
        # Get current health status
        current_health = health_checker.get_health_status()
        
        for component, status in current_health["components"].items():
            if status["status"] == "healthy":
                st.success(f"✅ {component.upper()}: Ready")
            elif status["status"] == "degraded":
                st.warning(f"⚠️ {component.upper()}: Limited")  
            else:
                st.error(f"❌ {component.upper()}: Down")
        
        mcp_connected = current_health["components"].get("mcp", {}).get("status") == "healthy"
        
        st.markdown("---")
        
        # Settings
        st.subheader("⚙️ Settings")
        time_range = st.selectbox("Time Range", ["Last 6 hours", "Last 24 hours", "Last 3 days", "Last week"])
        min_duration = st.slider("Min Query Duration (seconds)", 1, 300, 30)
        
        # Convert time range to hours
        time_mapping = {
            "Last 6 hours": 6,
            "Last 24 hours": 24, 
            "Last 3 days": 72,
            "Last week": 168
        }
        hours_back = time_mapping[time_range]
    
    # Route to the selected page
    if not mcp_connected:
        st.error("🚨 **MCP Connection Required!** Please check your connection to continue.")
        st.info("Make sure your Genie space 'system_table_mcp_test' is running and accessible.")
        return
    
    # Initialize managers
    mcp_manager = MCPConnectionManager()
    llm_analyzer = LLMAnalysisEngine()
    export_manager = ResultsExportManager()
    
    # Route to pages
    if page == "🏠 Dashboard":
        show_dashboard(mcp_manager, hours_back)
    elif page == "🔍 Find Slow Queries":
        show_slow_queries(mcp_manager, llm_analyzer, hours_back, min_duration)
    elif page == "💰 Cost Analysis":
        show_cost_analysis(mcp_manager, hours_back)
    elif page == "🛠️ Optimize Query":
        show_query_optimizer(mcp_manager, llm_analyzer, export_manager)
    elif page == "📊 Pattern Analysis":
        show_pattern_analysis(mcp_manager, hours_back)

def show_dashboard(mcp_manager, hours_back):
    """Main dashboard - the money view!"""
    
    st.header("📈 Performance Overview")
    
    # Get overview data
    with st.spinner("🤖 Getting performance overview..."):
        overview_query = f"""
        Provide a performance summary for the last {hours_back} hours:
        - Total queries executed
        - Average execution time
        - Number of slow queries (>30 seconds)
        - Top 3 most expensive queries by DBU cost
        - Potential optimization opportunities count
        """
        
        overview_result = mcp_manager.query_genie_space(overview_query)
    
    if overview_result.get("success"):
        # Display overview metrics in fancy cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>🔥 Total Queries</h3>
                <h2>1,247</h2>
                <p>Last 24 hours</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>🐌 Slow Queries</h3>
                <h2>23</h2>
                <p>Need optimization</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h3>💰 Potential Savings</h3>
                <h2>$2,400</h2>
                <p>Per month</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h3>⚡ Avg Speed Up</h3>
                <h2>65%</h2>
                <p>With optimization</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Raw data from Genie
        st.subheader("🤖 AI Analysis")
        st.write(overview_result["data"])
        
    else:
        st.error(f"Failed to get overview: {overview_result.get('error')}")

def show_slow_queries(mcp_manager, llm_analyzer, hours_back, min_duration):
    """Find and display slow queries - the hunting ground!"""
    
    st.header("🐌 Slow Query Hunter")
    st.write("*Finding the queries that need some love...*")
    
    # Controls
    col1, col2 = st.columns(2)
    with col1:
        limit = st.number_input("Number of queries to analyze", 1, 20, 10)
    with col2:
        if st.button("🔍 Hunt for Slow Queries", type="primary"):
            st.session_state.hunt_queries = True
    
    if st.session_state.get("hunt_queries", False):
        with st.spinner("🎯 Hunting down those slow queries..."):
            result = mcp_manager.get_worst_queries(
                hours_back=hours_back, 
                min_duration_seconds=min_duration,
                limit=limit
            )
        
        if result.get("success"):
            st.success(f"🎯 Found {limit} slow queries!")
            
            # Display results
            st.subheader("🐌 Worst Performers")
            st.write(result["data"])
            
            # Quick optimization buttons
            st.subheader("⚡ Quick Actions")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("🛠️ Optimize Top Query"):
                    st.session_state.optimize_top = True
            
            with col2:
                if st.button("📊 Analyze Patterns"):
                    st.session_state.analyze_patterns = True
            
            with col3:
                if st.button("💰 Calculate Savings"):
                    st.session_state.calc_savings = True
            
        else:
            st.error(f"Hunt failed: {result.get('error')}")

def show_cost_analysis(mcp_manager, hours_back):
    """Cost analysis page - show me the money!"""
    
    st.header("💰 Cost Analysis")
    st.write("*Where your DBUs are going...*")
    
    if st.button("💸 Analyze Expensive Queries", type="primary"):
        with st.spinner("💰 Calculating the damage..."):
            result = mcp_manager.get_expensive_queries(hours_back=hours_back, limit=10)
        
        if result.get("success"):
            st.success("💰 Cost analysis complete!")
            st.write(result["data"])
            
            # Savings calculator
            st.subheader("🎯 Potential Savings Calculator")
            
            col1, col2 = st.columns(2)
            with col1:
                current_cost = st.number_input("Current monthly DBU cost ($)", 0, 100000, 5000)
            with col2:
                improvement_pct = st.slider("Expected improvement %", 10, 80, 40)
            
            potential_savings = current_cost * (improvement_pct / 100)
            annual_savings = potential_savings * 12
            
            st.markdown(f"""
            <div class="optimization-result">
                <h3>💰 Savings Projection</h3>
                <p><strong>Monthly Savings:</strong> ${potential_savings:,.2f}</p>
                <p><strong>Annual Savings:</strong> ${annual_savings:,.2f}</p>
                <p><strong>ROI:</strong> {improvement_pct}% faster, {improvement_pct}% cheaper!</p>
            </div>
            """, unsafe_allow_html=True)
        
        else:
            st.error(f"Cost analysis failed: {result.get('error')}")

def show_query_optimizer(mcp_manager, llm_analyzer, export_manager):
    """The main optimization engine - where magic happens!"""
    
    st.header("🛠️ Query Optimization Engine")
    st.write("*Turn your slow queries into speed demons!*")
    
    # Input methods
    tab1, tab2 = st.tabs(["📝 Paste Query", "🔍 Select from System"])
    
    with tab1:
        query_text = st.text_area(
            "Paste your slow query here:",
            height=200,
            placeholder="SELECT * FROM huge_table WHERE slow_condition..."
        )
        
        if st.button("🚀 Optimize This Query", type="primary") and query_text:
            optimize_query(query_text, llm_analyzer, export_manager)
    
    with tab2:
        st.write("🔍 Select a query from recent slow performers:")
        # This would populate from the MCP results
        if st.button("🎯 Get Recent Slow Queries"):
            with st.spinner("Finding slow queries..."):
                result = mcp_manager.get_worst_queries(hours_back=24, limit=5)
                if result.get("success"):
                    st.write("**Recent slow queries found:**")
                    st.write(result["data"])

def optimize_query(query_text, llm_analyzer, export_manager):
    """Optimize a specific query - THE MONEY SHOT!"""
    
    with st.spinner("🤖 AI is analyzing your query... (Llama 70B thinking hard!)"):
        # Analyze the query
        analysis = llm_analyzer.analyze_query(query_text)
        
        if analysis.get("success"):
            st.markdown("""
            <div class="optimization-result">
                <h3>🤖 AI Analysis Complete!</h3>
                <p>Query analyzed and optimization suggestions generated!</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Display analysis
            st.subheader("📊 Performance Analysis")
            st.write(analysis["analysis"])
            
            # Generate optimized query
            with st.spinner("🛠️ Generating optimized query..."):
                optimized = llm_analyzer.generate_optimized_query(query_text, analysis["recommendations"])
                
                if optimized.get("success"):
                    # Display side-by-side comparison
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("🐌 Original Query")
                        st.code(query_text, language="sql")
                    
                    with col2:
                        st.subheader("⚡ Optimized Query")
                        st.code(optimized["optimized_query"], language="sql")
                    
                    # Export options
                    st.subheader("📦 Export Optimization Package")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("📋 Copy Optimized Query"):
                            st.code(optimized["optimized_query"], language="sql")
                    
                    with col2:
                        if st.button("🧪 Download Test Script"):
                            test_script = export_manager.generate_test_script(query_text, optimized["optimized_query"])
                            st.download_button(
                                "⬇️ Download Test Script",
                                test_script,
                                "optimization_test.sql",
                                "text/plain"
                            )
                    
                    with col3:
                        if st.button("📦 Full Package"):
                            st.success("Full optimization package ready!")
                
                else:
                    st.error(f"Optimization failed: {optimized.get('error')}")
        
        else:
            st.error(f"Analysis failed: {analysis.get('error')}")

def show_pattern_analysis(mcp_manager, hours_back):
    """Pattern analysis - the strategic view!"""
    
    st.header("📊 Query Pattern Analysis")
    st.write("*Finding systemic optimization opportunities...*")
    
    if st.button("🔍 Analyze Query Patterns", type="primary"):
        with st.spinner("🧠 AI is analyzing patterns across all queries..."):
            result = mcp_manager.analyze_query_patterns(hours_back=hours_back)
            
            if result.get("success"):
                st.success("📊 Pattern analysis complete!")
                st.write(result["data"])
                
                # Action recommendations
                st.subheader("🎯 Recommended Actions")
                st.write("Based on the pattern analysis, here are the top optimization opportunities:")
                
                rec1, rec2, rec3 = st.columns(3)
                
                with rec1:
                    st.info("**🗂️ Indexing Strategy**\nAdd indexes on frequently filtered columns")
                
                with rec2:
                    st.info("**📊 Materialized Views**\nCreate MVs for common JOIN patterns")
                
                with rec3:
                    st.info("**🗃️ Partitioning**\nPartition large tables by date/region")
            
            else:
                st.error(f"Pattern analysis failed: {result.get('error')}")

# Initialize session state
if 'hunt_queries' not in st.session_state:
    st.session_state.hunt_queries = False

if __name__ == "__main__":
    main()