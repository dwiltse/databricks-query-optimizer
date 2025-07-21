#!/usr/bin/env python3
"""
Demo Dashboard for Genie Space 2: Query Optimization Opportunities
Run with: streamlit run genie_space_2_dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

# Mock data that would come from Genie Space 2
@st.cache_data
def load_optimization_opportunities():
    """Mock data from mcp.query_optimization.mv_pattern_performance"""
    return pd.DataFrame([
        {"pattern_type": "select_all", "opportunity_count": 127, "total_potential_savings": 847, "avg_cost_dbu": 2.4, "affected_users": 23, "last_seen": "2024-01-15"},
        {"pattern_type": "unbounded_sort", "opportunity_count": 89, "total_potential_savings": 723, "avg_cost_dbu": 3.1, "affected_users": 18, "last_seen": "2024-01-15"},
        {"pattern_type": "cartesian_join", "opportunity_count": 23, "total_potential_savings": 1205, "avg_cost_dbu": 8.7, "affected_users": 7, "last_seen": "2024-01-14"},
        {"pattern_type": "missing_partition", "opportunity_count": 67, "total_potential_savings": 312, "avg_cost_dbu": 1.8, "affected_users": 15, "last_seen": "2024-01-15"},
        {"pattern_type": "redundant_distinct", "opportunity_count": 45, "total_potential_savings": 160, "avg_cost_dbu": 1.2, "affected_users": 12, "last_seen": "2024-01-13"}
    ])

@st.cache_data
def load_user_opportunities():
    """Mock data from mcp.query_optimization.mv_user_performance"""
    return pd.DataFrame([
        {"user_id": "analyst_john", "total_queries": 234, "optimization_opportunities": 45, "potential_savings_dbu": 67.3, "most_common_pattern": "select_all"},
        {"user_id": "data_team_mary", "total_queries": 189, "optimization_opportunities": 38, "potential_savings_dbu": 52.1, "most_common_pattern": "unbounded_sort"},
        {"user_id": "bi_developer", "total_queries": 156, "optimization_opportunities": 31, "potential_savings_dbu": 43.7, "most_common_pattern": "select_all"},
        {"user_id": "analyst_sarah", "total_queries": 98, "optimization_opportunities": 22, "potential_savings_dbu": 28.9, "most_common_pattern": "missing_partition"},
        {"user_id": "ml_engineer", "total_queries": 87, "optimization_opportunities": 18, "potential_savings_dbu": 19.4, "most_common_pattern": "redundant_distinct"}
    ])

@st.cache_data
def load_sample_queries():
    """Sample queries for the selected pattern"""
    return {
        "select_all": [
            {"query_id": "q_001", "query_text": "SELECT * FROM sales_data WHERE region = 'US' AND date >= '2024-01-01'", "cost_dbu": 2.8, "duration_ms": 45000},
            {"query_id": "q_002", "query_text": "SELECT * FROM customer_orders ORDER BY order_date DESC", "cost_dbu": 3.2, "duration_ms": 67000},
            {"query_id": "q_003", "query_text": "SELECT * FROM product_catalog WHERE category = 'Electronics'", "cost_dbu": 1.9, "duration_ms": 23000}
        ],
        "unbounded_sort": [
            {"query_id": "q_101", "query_text": "SELECT customer_id, amount FROM transactions ORDER BY amount DESC", "cost_dbu": 4.1, "duration_ms": 89000},
            {"query_id": "q_102", "query_text": "SELECT * FROM audit_log ORDER BY timestamp", "cost_dbu": 5.3, "duration_ms": 112000}
        ],
        "cartesian_join": [
            {"query_id": "q_201", "query_text": "SELECT * FROM table_a, table_b WHERE table_a.status = 'active'", "cost_dbu": 12.7, "duration_ms": 234000}
        ]
    }

def mock_mcp_analysis(pattern_type, sample_queries):
    """Mock MCP server response"""
    
    analyses = {
        "select_all": {
            "analysis": """
## 🎯 SELECT * Pattern Analysis

**From Genie Space 2:**
- Execution Count: 127 queries in last 30 days
- Average Cost: 2.4 DBU per query
- Estimated Savings: 35% performance improvement
- Users Affected: 23 different users

**From Knowledge Base (PDF Rules):**
- **Rule ID**: sel001 - "Avoid SELECT * in Production Queries"
- **Expected Improvement**: 30-50% reduction in data transfer and memory usage
- **Best Practice**: Only select columns you actually need

**ROI Analysis:**
- Monthly Savings: $264 (127 queries × 2.4 DBU × $0.22 × 35%)
- Implementation Effort: Low (5-10 minutes per query)
- Payback Period: Immediate
- Priority: HIGH

**Why This Matters:**
SELECT * reads all columns including large text fields, binary data, and unused dimensions. This increases I/O, memory usage, and network transfer unnecessarily.
            """,
            "optimized_queries": [
                {
                    "original": "SELECT * FROM sales_data WHERE region = 'US' AND date >= '2024-01-01'",
                    "optimized": "SELECT customer_id, product_name, sales_amount, order_date FROM sales_data WHERE region = 'US' AND date >= '2024-01-01'",
                    "improvement": "35% faster, 40% less data transfer"
                }
            ]
        },
        "unbounded_sort": {
            "analysis": """
## 🎯 Unbounded Sort Pattern Analysis

**From Genie Space 2:**
- Execution Count: 89 queries in last 30 days  
- Average Cost: 3.1 DBU per query
- Estimated Savings: 60% cost reduction
- Users Affected: 18 different users

**From Knowledge Base (PDF Rules):**
- **Rule ID**: limit001 - "Add LIMIT Clauses to Exploratory Queries"
- **Expected Improvement**: 50-90% reduction in data processed
- **Best Practice**: Always use LIMIT for ORDER BY in exploration

**ROI Analysis:**
- Monthly Savings: $364 (89 queries × 3.1 DBU × $0.22 × 60%)
- Implementation Effort: Very Low (2 minutes per query)
- Payback Period: Immediate
- Priority: HIGH

**Why This Matters:**
ORDER BY without LIMIT processes entire dataset just to return unlimited results. Adding LIMIT dramatically reduces compute needed.
            """,
            "optimized_queries": [
                {
                    "original": "SELECT customer_id, amount FROM transactions ORDER BY amount DESC",
                    "optimized": "SELECT customer_id, amount FROM transactions ORDER BY amount DESC LIMIT 1000",
                    "improvement": "70% faster, 80% less data processed"
                }
            ]
        }
    }
    
    return analyses.get(pattern_type, {"analysis": "Analysis not available for this pattern.", "optimized_queries": []})

# Main Dashboard
def main():
    st.set_page_config(page_title="Query Optimizer", page_icon="🚀", layout="wide")
    
    st.title("🚀 Databricks Query Optimizer")
    st.subheader("Genie Space 2: Query Optimization Opportunities")
    
    # Load data
    opportunities_df = load_optimization_opportunities()
    users_df = load_user_opportunities()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_queries = opportunities_df['opportunity_count'].sum()
    total_savings = opportunities_df['total_potential_savings'].sum()
    total_users = opportunities_df['affected_users'].sum()
    avg_savings_per_opportunity = total_savings / total_queries if total_queries > 0 else 0
    
    with col1:
        st.metric("Total Opportunities", f"{total_queries:,}")
    with col2:
        st.metric("Potential Monthly Savings", f"${total_savings:,.0f}")
    with col3:
        st.metric("Users Affected", f"{total_users}")
    with col4:
        st.metric("Avg Savings/Query", f"${avg_savings_per_opportunity:.2f}")
    
    st.markdown("---")
    
    # Main content in two columns
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("🎯 Optimization Opportunities")
        
        # Add priority classification
        def get_priority(savings):
            if savings >= 1000:
                return "🔴 CRITICAL"
            elif savings >= 500:
                return "🟡 HIGH" 
            elif savings >= 200:
                return "🟢 MEDIUM"
            else:
                return "⚪ LOW"
        
        opportunities_df['priority'] = opportunities_df['total_potential_savings'].apply(get_priority)
        
        # Display opportunities table
        display_df = opportunities_df.copy()
        display_df['pattern_type'] = display_df['pattern_type'].str.replace('_', ' ').str.title()
        display_df = display_df.rename(columns={
            'pattern_type': 'Pattern Type',
            'opportunity_count': 'Count',
            'avg_cost_dbu': 'Avg DBU',
            'total_potential_savings': 'Monthly Savings ($)',
            'affected_users': 'Users',
            'priority': 'Priority'
        })
        
        st.dataframe(
            display_df[['Pattern Type', 'Count', 'Avg DBU', 'Monthly Savings ($)', 'Users', 'Priority']], 
            use_container_width=True,
            hide_index=True
        )
        
        # Pattern selection for detailed analysis
        st.subheader("🔍 Pattern Deep Dive")
        
        selected_pattern = st.selectbox(
            "Select optimization pattern to analyze:",
            options=opportunities_df['pattern_type'].tolist(),
            format_func=lambda x: x.replace('_', ' ').title()
        )
        
        if selected_pattern:
            # Show sample queries for selected pattern
            sample_queries = load_sample_queries().get(selected_pattern, [])
            
            if sample_queries:
                st.write(f"### Sample {selected_pattern.replace('_', ' ').title()} Queries")
                
                for i, query in enumerate(sample_queries):
                    with st.expander(f"Query {i+1}: {query['query_id']} (Cost: {query['cost_dbu']} DBU, Duration: {query['duration_ms']/1000:.1f}s)"):
                        st.code(query['query_text'], language='sql')
                
                # MCP Analysis Button
                if st.button("🤖 Analyze with AI", type="primary"):
                    st.write("### 🤖 AI Analysis & Optimization Recommendations")
                    
                    # Show loading spinner
                    with st.spinner("Analyzing pattern with MCP server..."):
                        # Mock MCP call
                        mcp_response = mock_mcp_analysis(selected_pattern, sample_queries)
                    
                    # Display analysis
                    st.markdown(mcp_response['analysis'])
                    
                    # Display optimized queries
                    if mcp_response['optimized_queries']:
                        st.write("### ✅ Optimized Queries")
                        
                        for i, opt_query in enumerate(mcp_response['optimized_queries']):
                            st.write(f"**Query {i+1} Optimization:**")
                            
                            col_before, col_after = st.columns(2)
                            
                            with col_before:
                                st.write("❌ **Before:**")
                                st.code(opt_query['original'], language='sql')
                            
                            with col_after:
                                st.write("✅ **After:**")
                                st.code(opt_query['optimized'], language='sql')
                                
                                # Download button
                                st.download_button(
                                    label=f"📥 Download Optimized Query {i+1}",
                                    data=opt_query['optimized'],
                                    file_name=f"optimized_{selected_pattern}_query_{i+1}.sql",
                                    mime="text/plain"
                                )
                            
                            st.success(f"**Expected Improvement:** {opt_query['improvement']}")
                            st.markdown("---")
    
    with col_right:
        # Charts and additional insights
        st.subheader("📊 Insights")
        
        # Savings by pattern chart
        fig_savings = px.bar(
            opportunities_df, 
            x='total_potential_savings', 
            y='pattern_type',
            orientation='h',
            title="Potential Savings by Pattern",
            labels={'total_potential_savings': 'Monthly Savings ($)', 'pattern_type': 'Pattern Type'}
        )
        fig_savings.update_layout(height=300)
        st.plotly_chart(fig_savings, use_container_width=True)
        
        # Top users
        st.subheader("👥 Users with Most Opportunities")
        
        top_users = users_df.nlargest(5, 'optimization_opportunities')[['user_id', 'optimization_opportunities', 'potential_savings_dbu']]
        top_users['potential_savings_usd'] = (top_users['potential_savings_dbu'] * 0.22).round(2)
        
        for _, user in top_users.iterrows():
            st.write(f"**{user['user_id']}**")
            st.write(f"  • {user['optimization_opportunities']} opportunities")
            st.write(f"  • ${user['potential_savings_usd']:.2f} potential savings")
            st.write("")
        
        # Implementation timeline
        st.subheader("🗓️ Suggested Timeline")
        st.write("**Week 1:** Critical patterns (Cartesian joins)")
        st.write("**Week 2:** High-impact patterns (SELECT *, unbounded sorts)")
        st.write("**Week 3:** Medium-impact patterns")
        st.write("**Week 4:** User training and monitoring")

if __name__ == "__main__":
    main()