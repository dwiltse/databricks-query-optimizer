#!/usr/bin/env python3
"""
POC Demo: Top 3 Worst Queries with Actionable Optimizations
Simple CLI demo for identifying worst queries and getting code updates
"""

import json
from datetime import datetime
from typing import List, Dict

# Mock data representing worst queries from Genie Space 2
WORST_QUERIES = [
    {
        "statement_id": "q_2024_001",
        "statement_text": "SELECT * FROM orders WHERE region = 'US' ORDER BY order_amount DESC",
        "executed_by": "analyst_john",
        "execution_duration_ms": 252000,  # 4.2 minutes
        "read_bytes": 2150000000,  # 2.1 GB
        "read_rows": 1500000,
        "estimated_cost_usd": 1.23,
        "query_name": "Daily Sales Report"
    },
    {
        "statement_id": "q_2024_002", 
        "statement_text": "SELECT * FROM customer_data cd, order_history oh WHERE cd.status = 'active'",
        "executed_by": "data_team_mary",
        "execution_duration_ms": 340000,  # 5.7 minutes
        "read_bytes": 4200000000,  # 4.2 GB
        "read_rows": 3200000,
        "estimated_cost_usd": 2.15,
        "query_name": "Customer Analysis Report"
    },
    {
        "statement_id": "q_2024_003",
        "statement_text": "SELECT DISTINCT customer_id, product_name FROM sales_data WHERE amount > 100",
        "executed_by": "bi_developer", 
        "execution_duration_ms": 185000,  # 3.1 minutes
        "read_bytes": 1800000000,  # 1.8 GB
        "read_rows": 2100000,
        "estimated_cost_usd": 0.89,
        "query_name": "High Value Customer List"
    }
]

class QueryOptimizer:
    """Mock MCP server for query optimization"""
    
    def __init__(self):
        # Optimization rules from PDF knowledge base
        self.optimization_rules = {
            "select_star": {
                "pattern": "SELECT *",
                "improvement": "30-50% reduction in data transfer",
                "action": "Replace with specific columns"
            },
            "missing_limit": {
                "pattern": "ORDER BY.*(?!LIMIT)",
                "improvement": "50-90% reduction in processing time", 
                "action": "Add LIMIT clause for exploration"
            },
            "cartesian_join": {
                "pattern": "FROM.*,.*WHERE(?!.*=)",
                "improvement": "80-95% reduction in compute cost",
                "action": "Replace with proper JOIN syntax"
            },
            "unnecessary_distinct": {
                "pattern": "SELECT DISTINCT",
                "improvement": "20-40% reduction in processing time",
                "action": "Remove DISTINCT if not needed or use GROUP BY"
            }
        }
    
    def analyze_query(self, query_data: Dict) -> Dict:
        """Analyze query and provide optimization recommendations"""
        
        query_text = query_data["statement_text"]
        problems = []
        optimizations = []
        
        # Detect problems and generate optimized SQL
        if "SELECT *" in query_text:
            problems.append("Uses SELECT * (reads unnecessary columns)")
            optimizations.append("select_star")
        
        if "ORDER BY" in query_text and "LIMIT" not in query_text:
            problems.append("Missing LIMIT clause on ORDER BY")
            optimizations.append("missing_limit")
        
        if " FROM " in query_text and "," in query_text and "JOIN" not in query_text:
            problems.append("Cartesian join detected (missing JOIN condition)")
            optimizations.append("cartesian_join")
        
        if "SELECT DISTINCT" in query_text:
            problems.append("Using DISTINCT - may be unnecessary")
            optimizations.append("unnecessary_distinct")
        
        # Generate optimized SQL
        optimized_sql = self._generate_optimized_sql(query_text, optimizations)
        
        # Calculate savings
        savings = self._calculate_savings(query_data, optimizations)
        
        return {
            "query_id": query_data["statement_id"],
            "query_name": query_data["query_name"],
            "original_sql": query_text,
            "problems_identified": problems,
            "optimized_sql": optimized_sql,
            "savings_estimate": savings,
            "implementation": self._generate_implementation_steps(query_data)
        }
    
    def _generate_optimized_sql(self, original_sql: str, optimizations: List[str]) -> str:
        """Generate optimized SQL based on detected issues"""
        
        # Simple SQL optimization examples
        if "q_2024_001" in original_sql or "orders" in original_sql:
            return """-- OPTIMIZED VERSION
SELECT 
    customer_id, 
    order_amount, 
    order_date,
    product_id
FROM orders 
WHERE region = 'US'
    AND order_date >= CURRENT_DATE - INTERVAL 30 DAY  -- Partition filter
ORDER BY order_amount DESC 
LIMIT 1000  -- Added limit for exploration"""
        
        elif "q_2024_002" in original_sql or ("customer_data" in original_sql and "order_history" in original_sql):
            return """-- OPTIMIZED VERSION  
SELECT 
    cd.customer_id,
    cd.customer_name,
    cd.status,
    oh.order_count,
    oh.total_amount
FROM customer_data cd
INNER JOIN (
    SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_amount
    FROM order_history 
    GROUP BY customer_id
) oh ON cd.customer_id = oh.customer_id
WHERE cd.status = 'active'"""
        
        elif "DISTINCT" in original_sql:
            return """-- OPTIMIZED VERSION
SELECT 
    customer_id, 
    product_name
FROM sales_data 
WHERE amount > 100
GROUP BY customer_id, product_name  -- More efficient than DISTINCT"""
        
        else:
            return "-- Optimized version would be generated based on specific patterns detected"
    
    def _calculate_savings(self, query_data: Dict, optimizations: List[str]) -> Dict:
        """Calculate estimated savings based on optimizations"""
        
        current_duration_ms = query_data["execution_duration_ms"]
        current_cost = query_data["estimated_cost_usd"]
        
        # Estimate improvement based on optimization types
        improvement_factor = 1.0
        for opt in optimizations:
            if opt == "select_star":
                improvement_factor *= 0.65  # 35% improvement
            elif opt == "missing_limit":
                improvement_factor *= 0.25  # 75% improvement  
            elif opt == "cartesian_join":
                improvement_factor *= 0.15  # 85% improvement
            elif opt == "unnecessary_distinct":
                improvement_factor *= 0.75  # 25% improvement
        
        new_duration_ms = int(current_duration_ms * improvement_factor)
        new_cost = current_cost * improvement_factor
        
        # Calculate monthly savings (assume run daily)
        monthly_savings = (current_cost - new_cost) * 30
        
        return {
            "current_duration": f"{current_duration_ms / 60000:.1f} minutes",
            "estimated_duration": f"{new_duration_ms / 60000:.1f} minutes",
            "time_savings": f"{int((1 - improvement_factor) * 100)}% faster",
            "current_cost": f"${current_cost:.2f} per run",
            "estimated_cost": f"${new_cost:.2f} per run",
            "monthly_savings": f"${monthly_savings:.2f}",
            "dbu_reduction": f"{current_cost/0.22:.1f} → {new_cost/0.22:.1f} DBU per execution"
        }
    
    def _generate_implementation_steps(self, query_data: Dict) -> Dict:
        """Generate deployment steps for the optimization"""
        
        query_name_slug = query_data["query_name"].lower().replace(" ", "-")
        
        return {
            "file_path": f"/Workspace/analytics/{query_name_slug.replace('-report', '')}.sql",
            "git_branch": f"optimize-{query_name_slug}",
            "testing_steps": [
                f"1. Create branch: git checkout -b optimize-{query_name_slug}",
                f"2. Update query in {query_name_slug.replace('-report', '')}.sql",
                "3. Test with sample data: spark.sql(optimized_query).show(10)",
                "4. Validate results match original query output",
                "5. Run performance comparison test",
                "6. Commit and create PR for review"
            ]
        }

def print_analysis_report(analysis: Dict):
    """Print formatted analysis report"""
    
    print(f"\n📊 QUERY #{analysis['query_id']}: {analysis['query_name']}")
    print("━" * 60)
    
    print(f"\n❌ CURRENT PERFORMANCE:")
    savings = analysis['savings_estimate']
    print(f"   • Duration: {savings['current_duration']}")
    print(f"   • Cost: {savings['current_cost']}")
    print(f"   • Problems: {', '.join(analysis['problems_identified'])}")
    
    print(f"\n✅ OPTIMIZED VERSION:")
    print(f"   • Duration: {savings['estimated_duration']} ({savings['time_savings']})")
    print(f"   • Cost: {savings['estimated_cost']} (vs {savings['current_cost']})")
    print(f"   • Monthly Savings: {savings['monthly_savings']} (if run daily)")
    print(f"   • DBU Reduction: {savings['dbu_reduction']}")
    
    print(f"\n📝 CODE TO DEPLOY:")
    print("```sql")
    print(analysis['optimized_sql'])
    print("```")
    
    print(f"\n🚀 DEPLOYMENT STEPS:")
    impl = analysis['implementation']
    print(f"   File: {impl['file_path']}")
    print(f"   Branch: {impl['git_branch']}")
    for step in impl['testing_steps']:
        print(f"   {step}")
    
    print("\n" + "━" * 60)

def main():
    """Run POC demo"""
    
    print("🔍 DATABRICKS QUERY OPTIMIZER - POC DEMO")
    print("🎯 TOP 3 WORST PERFORMING QUERIES")
    print("=" * 60)
    
    optimizer = QueryOptimizer()
    total_monthly_savings = 0
    
    # Analyze each of the worst queries
    for i, query_data in enumerate(WORST_QUERIES, 1):
        analysis = optimizer.analyze_query(query_data)
        print_analysis_report(analysis)
        
        # Extract monthly savings for total
        monthly_savings_str = analysis['savings_estimate']['monthly_savings']
        monthly_savings = float(monthly_savings_str.replace('$', ''))
        total_monthly_savings += monthly_savings
    
    # Summary
    print(f"\n💰 TOTAL POTENTIAL MONTHLY SAVINGS: ${total_monthly_savings:.2f}")
    print(f"🎯 TOTAL IMPLEMENTATION TIME: ~2-3 hours")
    print(f"📈 ROI: ${total_monthly_savings:.2f}/month ÷ 3 hours = ${total_monthly_savings/3:.2f}/hour value")
    print(f"\n⚡ NEXT STEPS:")
    print("1. Review optimized queries with data team")
    print("2. Test optimizations in development environment") 
    print("3. Deploy highest-impact optimization first (Query #2)")
    print("4. Monitor performance improvements")
    print("5. Expand to next batch of queries")

if __name__ == "__main__":
    main()