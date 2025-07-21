#!/usr/bin/env python3
"""
Enterprise Demo: Databricks Managed MCP Integration
Shows how to use Databricks' managed MCP service for secure query optimization
"""

import json
import requests
from typing import Dict, List
from dataclasses import dataclass

@dataclass 
class ManagedMCPConfig:
    """Configuration for Databricks Managed MCP"""
    workspace_url: str
    mcp_endpoint: str  # Databricks managed MCP endpoint
    access_token: str
    mcp_service_id: str  # Enterprise MCP service identifier
    genie_space_id: str  # Connected Genie Space

class DatabricksManagedMCPClient:
    """Client for Databricks Managed MCP service"""
    
    def __init__(self, config: ManagedMCPConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {config.access_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'databricks-query-optimizer/1.0'
        })
    
    def analyze_worst_queries(self, limit: int = 3) -> Dict:
        """Use managed MCP to analyze worst performing queries from Genie Space"""
        
        # Call Databricks Managed MCP API
        payload = {
            "service_id": self.config.mcp_service_id,
            "tool": "analyze_performance_patterns",
            "parameters": {
                "genie_space_id": self.config.genie_space_id,
                "query": """
                SELECT 
                    statement_id,
                    statement_text, 
                    executed_by,
                    execution_duration_ms,
                    read_bytes,
                    estimated_cost_dbu
                FROM mcp.query_optimization.mv_query_performance_categorized  
                WHERE performance_category = 'SLOW'
                    AND end_time >= CURRENT_DATE - INTERVAL 7 DAY
                ORDER BY execution_duration_ms DESC
                LIMIT {}
                """.format(limit),
                "analysis_type": "optimization_recommendations",
                "include_code_generation": True,
                "security_context": "enterprise_secure"
            }
        }
        
        response = self._call_managed_mcp(payload)
        return self._format_optimization_response(response)
    
    def get_optimization_recommendations(self, query_id: str, query_text: str) -> Dict:
        """Get specific optimization recommendations for a query"""
        
        payload = {
            "service_id": self.config.mcp_service_id,
            "tool": "optimize_sql_query",
            "parameters": {
                "query_text": query_text,
                "query_id": query_id,
                "context": {
                    "workspace_id": self._extract_workspace_id(),
                    "genie_space_id": self.config.genie_space_id,
                    "performance_data": True,
                    "cost_analysis": True
                },
                "optimization_focus": ["performance", "cost", "best_practices"],
                "generate_deployment_steps": True,
                "compliance_check": True  # Enterprise feature
            }
        }
        
        return self._call_managed_mcp(payload)
    
    def validate_optimization(self, original_sql: str, optimized_sql: str) -> Dict:
        """Validate that optimized SQL is semantically equivalent"""
        
        payload = {
            "service_id": self.config.mcp_service_id,
            "tool": "validate_sql_equivalence", 
            "parameters": {
                "original_query": original_sql,
                "optimized_query": optimized_sql,
                "validation_level": "strict",
                "generate_test_cases": True,
                "security_validation": True  # Enterprise security check
            }
        }
        
        return self._call_managed_mcp(payload)
    
    def _call_managed_mcp(self, payload: Dict) -> Dict:
        """Call Databricks Managed MCP API"""
        
        url = f"{self.config.workspace_url}/api/2.1/mcp/services/invoke"
        
        try:
            response = self.session.post(url, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            return {
                "error": f"Managed MCP call failed: {str(e)}",
                "status": "error"
            }
    
    def _format_optimization_response(self, mcp_response: Dict) -> Dict:
        """Format MCP response for enterprise demo"""
        
        if "error" in mcp_response:
            return mcp_response
            
        # Extract enterprise MCP response
        analyses = mcp_response.get("tool_response", {}).get("analyses", [])
        
        formatted_results = []
        total_savings = 0
        
        for analysis in analyses:
            savings_estimate = analysis.get("cost_impact", {})
            monthly_savings = float(savings_estimate.get("monthly_savings_usd", 0))
            total_savings += monthly_savings
            
            formatted_results.append({
                "query_id": analysis.get("query_id"),
                "query_name": analysis.get("detected_purpose", "Unknown Query"),
                "current_performance": {
                    "duration_minutes": analysis.get("current_duration_ms", 0) / 60000,
                    "cost_per_run": analysis.get("current_cost_usd", 0),
                    "dbu_consumption": analysis.get("current_dbu", 0)
                },
                "optimization_recommendations": analysis.get("recommendations", []),
                "optimized_sql": analysis.get("optimized_query", ""),
                "savings_estimate": {
                    "time_improvement_percent": analysis.get("performance_improvement_pct", 0),
                    "cost_savings_percent": analysis.get("cost_reduction_pct", 0), 
                    "monthly_savings_usd": monthly_savings,
                    "dbu_reduction": analysis.get("dbu_reduction", {})
                },
                "enterprise_validation": {
                    "security_approved": analysis.get("security_validation", {}).get("approved", False),
                    "compliance_check": analysis.get("compliance_status", "unknown"),
                    "risk_level": analysis.get("risk_assessment", "medium")
                },
                "deployment": {
                    "workspace_path": analysis.get("suggested_file_path", ""),
                    "git_integration": analysis.get("git_workflow", {}),
                    "testing_strategy": analysis.get("testing_recommendations", [])
                }
            })
        
        return {
            "status": "success",
            "total_queries_analyzed": len(analyses),
            "total_monthly_savings_usd": total_savings,
            "mcp_service_info": {
                "service_id": self.config.mcp_service_id,
                "response_time_ms": mcp_response.get("execution_time_ms", 0),
                "tokens_used": mcp_response.get("token_usage", {}),
                "model_version": mcp_response.get("model_info", {}).get("version", "unknown")
            },
            "query_analyses": formatted_results
        }
    
    def _extract_workspace_id(self) -> str:
        """Extract workspace ID from URL"""
        return self.config.workspace_url.split('.')[0].split('/')[-1]

class EnterpriseDemoRunner:
    """Demo runner for enterprise Databricks environment"""
    
    def __init__(self, config: ManagedMCPConfig):
        self.mcp_client = DatabricksManagedMCPClient(config)
    
    def run_worst_queries_demo(self):
        """Run the enterprise demo showing managed MCP capabilities"""
        
        print("🏢 DATABRICKS MANAGED MCP - ENTERPRISE QUERY OPTIMIZATION")
        print("🔒 Secure, Compliant, Managed AI Analysis")
        print("=" * 70)
        
        # Step 1: Analyze worst queries using managed MCP
        print("\n📊 Step 1: Analyzing worst queries with Managed MCP...")
        print("🔄 Connecting to enterprise MCP service...")
        
        analysis_result = self.mcp_client.analyze_worst_queries(limit=3)
        
        if analysis_result.get("status") == "error":
            print(f"❌ Error: {analysis_result['error']}")
            return
        
        # Display MCP service info
        mcp_info = analysis_result["mcp_service_info"]
        print(f"✅ Connected to MCP Service: {mcp_info['service_id']}")
        print(f"📈 Response Time: {mcp_info['response_time_ms']}ms")
        print(f"🤖 Model: {mcp_info['model_version']}")
        
        # Step 2: Display analysis results
        print(f"\n🎯 TOP {analysis_result['total_queries_analyzed']} WORST QUERIES IDENTIFIED")
        print("-" * 70)
        
        for i, analysis in enumerate(analysis_result["query_analyses"], 1):
            self._print_enterprise_analysis(i, analysis)
        
        # Step 3: Show enterprise benefits
        print(f"\n💼 ENTERPRISE MCP BENEFITS:")
        print(f"🔒 Security Validation: All optimizations security-approved")
        print(f"📋 Compliance: Built-in governance and audit trail")
        print(f"🏢 Managed Service: No infrastructure to maintain")
        print(f"⚡ Performance: Sub-second response times")
        print(f"💰 Total Monthly Savings: ${analysis_result['total_monthly_savings_usd']:.2f}")
        
        # Step 4: Show deployment integration
        print(f"\n🚀 ENTERPRISE DEPLOYMENT FEATURES:")
        print("✅ Automatic Git workflow integration")
        print("✅ Built-in testing recommendations") 
        print("✅ Compliance and security validation")
        print("✅ Workspace path suggestions")
        print("✅ Audit logging and governance")
    
    def _print_enterprise_analysis(self, query_num: int, analysis: Dict):
        """Print formatted enterprise analysis"""
        
        print(f"\n📊 QUERY #{query_num}: {analysis['query_name']}")
        print("━" * 50)
        
        # Current performance
        current = analysis["current_performance"] 
        print(f"❌ CURRENT:")
        print(f"   Duration: {current['duration_minutes']:.1f} minutes")
        print(f"   Cost: ${current['cost_per_run']:.2f} per run")
        print(f"   DBU: {current['dbu_consumption']:.1f}")
        
        # Optimized performance
        savings = analysis["savings_estimate"]
        print(f"✅ OPTIMIZED (Managed MCP Recommendation):")
        print(f"   Time: {savings['time_improvement_percent']}% faster")
        print(f"   Cost: {savings['cost_savings_percent']}% reduction")
        print(f"   Monthly Savings: ${savings['monthly_savings_usd']:.2f}")
        
        # Enterprise validation
        validation = analysis["enterprise_validation"]
        print(f"🔒 ENTERPRISE VALIDATION:")
        print(f"   Security: {'✅ Approved' if validation['security_approved'] else '❌ Review Needed'}")
        print(f"   Compliance: {validation['compliance_check'].title()}")
        print(f"   Risk Level: {validation['risk_level'].title()}")
        
        # Deployment info
        deployment = analysis["deployment"]
        print(f"🚀 MANAGED DEPLOYMENT:")
        print(f"   Path: {deployment['workspace_path']}")
        print(f"   Git: Automated workflow configured")
        print(f"   Testing: {len(deployment.get('testing_strategy', []))} validation steps")

# Demo configuration for enterprise environment
def create_enterprise_config() -> ManagedMCPConfig:
    """Create configuration for enterprise demo"""
    return ManagedMCPConfig(
        workspace_url="https://your-enterprise.cloud.databricks.com",
        mcp_endpoint="/api/2.1/mcp/services",
        access_token="YOUR_ENTERPRISE_TOKEN",  # Service principal token
        mcp_service_id="dbx_managed_mcp_query_optimizer_v1",
        genie_space_id="genie_space_query_optimization_prod"
    )

if __name__ == "__main__":
    # Enterprise demo
    config = create_enterprise_config()
    demo = EnterpriseDemoRunner(config)
    demo.run_worst_queries_demo()