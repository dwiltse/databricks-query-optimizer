"""
Health Check Module for Databricks Apps
Ensures all components are working before serving traffic
"""

import time
from mcp_manager import MCPConnectionManager
from llm_analyzer import LLMAnalysisEngine

class HealthChecker:
    """Check health of all app components"""
    
    def __init__(self):
        self.startup_time = time.time()
        self.mcp_manager = None
        self.llm_analyzer = None
        self.last_health_check = None
        self.health_status = {"status": "starting", "components": {}}
    
    def perform_startup_checks(self, max_retries=3, retry_delay=5):
        """Perform startup health checks with retries"""
        print("🏥 Starting health checks...")
        
        # Check MCP connection
        for attempt in range(max_retries):
            try:
                print(f"🔍 MCP health check (attempt {attempt + 1}/{max_retries})")
                self.mcp_manager = MCPConnectionManager()
                mcp_status = self.mcp_manager.test_connection()
                
                if mcp_status["status"] == "success":
                    self.health_status["components"]["mcp"] = {
                        "status": "healthy",
                        "message": mcp_status["message"],
                        "tools_count": len(mcp_status.get("tools", []))
                    }
                    break
                else:
                    print(f"⚠️ MCP check failed: {mcp_status['message']}")
                    if attempt < max_retries - 1:
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        self.health_status["components"]["mcp"] = {
                            "status": "unhealthy",
                            "error_type": mcp_status.get("error_type", "unknown"),
                            "message": mcp_status["message"],
                            "troubleshooting": mcp_status.get("troubleshooting", [])
                        }
            except Exception as e:
                print(f"❌ MCP health check exception: {e}")
                if attempt == max_retries - 1:
                    self.health_status["components"]["mcp"] = {
                        "status": "error",
                        "message": f"Health check failed: {str(e)}"
                    }
        
        # Check LLM connection
        for attempt in range(max_retries):
            try:
                print(f"🧠 LLM health check (attempt {attempt + 1}/{max_retries})")
                self.llm_analyzer = LLMAnalysisEngine()
                llm_status = self.llm_analyzer.test_llm_connection()
                
                if llm_status["status"] == "success":
                    self.health_status["components"]["llm"] = {
                        "status": "healthy",
                        "message": llm_status["message"],
                        "model": self.llm_analyzer.model
                    }
                    break
                else:
                    print(f"⚠️ LLM check failed: {llm_status['message']}")
                    if attempt < max_retries - 1:
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        self.health_status["components"]["llm"] = {
                            "status": "unhealthy", 
                            "message": llm_status["message"]
                        }
            except Exception as e:
                print(f"❌ LLM health check exception: {e}")
                if attempt == max_retries - 1:
                    self.health_status["components"]["llm"] = {
                        "status": "error",
                        "message": f"Health check failed: {str(e)}"
                    }
        
        # Determine overall status
        component_statuses = [comp["status"] for comp in self.health_status["components"].values()]
        
        if all(status == "healthy" for status in component_statuses):
            self.health_status["status"] = "healthy"
            self.health_status["message"] = "All components operational"
        elif any(status == "healthy" for status in component_statuses):
            self.health_status["status"] = "degraded" 
            self.health_status["message"] = "Some components operational"
        else:
            self.health_status["status"] = "unhealthy"
            self.health_status["message"] = "Critical components unavailable"
        
        self.health_status["startup_time"] = time.time() - self.startup_time
        self.last_health_check = time.time()
        
        print(f"🏥 Health check complete: {self.health_status['status']}")
        return self.health_status
    
    def get_health_status(self):
        """Get current health status"""
        return self.health_status
    
    def is_ready(self):
        """Check if app is ready to serve traffic"""
        return self.health_status["status"] in ["healthy", "degraded"]

# Global health checker instance
health_checker = HealthChecker()

def get_health_checker():
    """Get the global health checker instance"""
    return health_checker