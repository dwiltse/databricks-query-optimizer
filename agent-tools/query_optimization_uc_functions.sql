-- Query Optimization Unity Catalog Functions for Databricks Agent Framework
-- These functions can be used as tools in the AI Playground
-- Deploy these to your mcp.query_optimization schema

-- =============================================================================
-- 1. Get Slowest Queries
-- =============================================================================
CREATE OR REPLACE FUNCTION mcp.query_optimization.get_slowest_queries(
  hours_back INT DEFAULT 24,
  limit_results INT DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Get the slowest queries from system tables within specified time range'
AS $$
import json
from datetime import datetime, timedelta

# This function analyzes system.query.history to find slowest queries
def get_slowest_queries(hours_back, limit_results):
    try:
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        # Query system tables (this would be executed in Databricks context)
        query = f"""
        SELECT 
            query_id,
            statement_text,
            execution_duration_ms,
            total_duration_ms,
            user_name,
            warehouse_id,
            start_time,
            rows_read,
            bytes_read,
            compute_cost_estimate
        FROM system.query.history 
        WHERE start_time >= '{start_time.isoformat()}'
            AND execution_duration_ms IS NOT NULL
            AND statement_type = 'SELECT'
        ORDER BY execution_duration_ms DESC
        LIMIT {limit_results}
        """
        
        # In actual implementation, this would execute the query
        results = {
            "query": query,
            "description": f"Top {limit_results} slowest queries in the last {hours_back} hours",
            "analysis": "These queries should be prioritized for optimization due to high execution time",
            "recommendations": [
                "Check for missing indexes on frequently filtered columns",
                "Consider query rewriting to reduce complexity", 
                "Evaluate if results can be cached or pre-computed",
                "Review table statistics and consider ANALYZE TABLE commands"
            ]
        }
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to get slowest queries: {str(e)}"})

return get_slowest_queries(hours_back, limit_results)
$$;

-- =============================================================================
-- 2. Get Most Expensive Queries (by DBU cost)
-- =============================================================================
CREATE OR REPLACE FUNCTION mcp.query_optimization.get_expensive_queries(
  hours_back INT DEFAULT 24,
  limit_results INT DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Get the most expensive queries by DBU consumption'
AS $$
import json
from datetime import datetime, timedelta

def get_expensive_queries(hours_back, limit_results):
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        query = f"""
        SELECT 
            qh.query_id,
            qh.statement_text,
            qh.execution_duration_ms,
            qh.user_name,
            qh.warehouse_id,
            qh.start_time,
            qh.rows_read,
            qh.bytes_read,
            bu.usage_quantity as dbu_consumed,
            bu.usage_quantity * 0.07 as estimated_cost_usd
        FROM system.query.history qh
        LEFT JOIN system.billing.usage bu ON qh.query_id = bu.custom_tags.query_id
        WHERE qh.start_time >= '{start_time.isoformat()}'
            AND bu.usage_quantity IS NOT NULL
            AND qh.statement_type = 'SELECT'
        ORDER BY bu.usage_quantity DESC
        LIMIT {limit_results}
        """
        
        results = {
            "query": query,
            "description": f"Top {limit_results} most expensive queries by DBU cost in the last {hours_back} hours",
            "analysis": "These queries consume the most compute resources and offer highest optimization ROI",
            "optimization_strategies": [
                "Implement result caching for frequently run expensive queries",
                "Consider materializing commonly accessed data",
                "Optimize JOIN operations and filter predicates",
                "Review warehouse sizing - smaller warehouses for simpler queries"
            ]
        }
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to get expensive queries: {str(e)}"})

return get_expensive_queries(hours_back, limit_results)
$$;

-- =============================================================================
-- 3. Analyze Query Patterns
-- =============================================================================
CREATE OR REPLACE FUNCTION mcp.query_optimization.analyze_query_patterns(
  hours_back INT DEFAULT 168  -- Default 1 week
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Analyze common query patterns and suggest optimization opportunities'
AS $$
import json
import re
from datetime import datetime, timedelta

def analyze_query_patterns(hours_back):
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        analysis_query = f"""
        WITH query_patterns AS (
          SELECT 
            -- Extract table names from queries
            regexp_extract_all(upper(statement_text), r'FROM\\s+([\\w\\.]+)', 1) as tables_used,
            -- Extract JOIN patterns
            regexp_extract_all(upper(statement_text), r'(INNER|LEFT|RIGHT|FULL)\\s+JOIN', 1) as join_types,
            -- Extract WHERE clause complexity
            length(regexp_extract(statement_text, r'WHERE.*', 0)) as where_complexity,
            execution_duration_ms,
            rows_read,
            user_name,
            start_time
          FROM system.query.history 
          WHERE start_time >= '{start_time.isoformat()}'
            AND statement_type = 'SELECT'
            AND execution_duration_ms > 1000  -- Focus on queries > 1 second
        )
        SELECT 
          COUNT(*) as total_queries,
          AVG(execution_duration_ms) as avg_duration_ms,
          COUNT(DISTINCT user_name) as unique_users,
          -- Most commonly accessed tables
          flatten(collect_list(tables_used)) as all_tables,
          -- JOIN usage patterns
          flatten(collect_list(join_types)) as all_joins,
          AVG(where_complexity) as avg_where_complexity
        FROM query_patterns
        """
        
        results = {
            "query": analysis_query,
            "description": f"Query pattern analysis for the last {hours_back} hours",
            "insights": [
                "Identify most frequently accessed tables for optimization priority",
                "Analyze JOIN patterns to suggest indexing strategies",
                "Complex WHERE clauses may benefit from predicate pushdown",
                "High-frequency patterns are good candidates for materialized views"
            ],
            "optimization_recommendations": {
                "materialized_views": "Create MVs for frequently accessed table combinations",
                "indexing": "Add indexes on commonly joined and filtered columns",
                "query_caching": "Enable result caching for repetitive query patterns",
                "warehouse_optimization": "Use smaller warehouses for simple, frequent queries"
            }
        }
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to analyze query patterns: {str(e)}"})

return analyze_query_patterns(hours_back)
$$;

-- =============================================================================
-- 4. Get Query Performance Baseline
-- =============================================================================
CREATE OR REPLACE FUNCTION mcp.query_optimization.get_performance_baseline(
  query_text STRING,
  days_back INT DEFAULT 30
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Get historical performance baseline for a specific query pattern'
AS $$
import json
from datetime import datetime, timedelta

def get_performance_baseline(query_text, days_back):
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        
        # Create a normalized version of the query for pattern matching
        baseline_query = f"""
        WITH normalized_queries AS (
          SELECT 
            query_id,
            -- Normalize query text for pattern matching
            regexp_replace(
              regexp_replace(statement_text, r'\\b\\d+\\b', 'N'), -- Replace numbers
              r"'[^']*'", "'X'"  -- Replace string literals
            ) as normalized_text,
            execution_duration_ms,
            total_duration_ms,
            rows_read,
            bytes_read,
            start_time,
            user_name
          FROM system.query.history 
          WHERE start_time >= '{start_time.isoformat()}'
            AND statement_type = 'SELECT'
        ),
        matching_queries AS (
          SELECT *
          FROM normalized_queries
          WHERE similarity(normalized_text, '{query_text}') > 0.7  -- 70% similarity threshold
        )
        SELECT 
          COUNT(*) as execution_count,
          AVG(execution_duration_ms) as avg_duration_ms,
          MIN(execution_duration_ms) as min_duration_ms,
          MAX(execution_duration_ms) as max_duration_ms,
          PERCENTILE(execution_duration_ms, 0.5) as median_duration_ms,
          PERCENTILE(execution_duration_ms, 0.95) as p95_duration_ms,
          AVG(rows_read) as avg_rows_read,
          AVG(bytes_read) as avg_bytes_read,
          COUNT(DISTINCT user_name) as unique_users
        FROM matching_queries
        """
        
        results = {
            "query": baseline_query,
            "description": f"Performance baseline for similar queries over the last {days_back} days",
            "baseline_metrics": {
                "execution_frequency": "How often this pattern runs",
                "performance_distribution": "Min, avg, max, median, p95 execution times",
                "data_volume": "Typical rows/bytes processed",
                "user_adoption": "Number of users running similar queries"
            },
            "optimization_insights": [
                "Compare current query performance against baseline",
                "Identify performance regressions over time",
                "Prioritize optimization based on execution frequency",
                "Set performance SLAs based on historical data"
            ]
        }
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to get performance baseline: {str(e)}"})

return get_performance_baseline(query_text, days_back)
$$;

-- =============================================================================
-- 5. Calculate Optimization ROI
-- =============================================================================
CREATE OR REPLACE FUNCTION mcp.query_optimization.calculate_optimization_roi(
  query_id STRING,
  proposed_improvement_percent FLOAT DEFAULT 30.0
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Calculate potential ROI from optimizing a specific query'
AS $$
import json
from datetime import datetime, timedelta

def calculate_optimization_roi(query_id, proposed_improvement_percent):
    try:
        # Get query details and historical execution frequency
        roi_query = f"""
        WITH query_details AS (
          SELECT 
            query_id,
            statement_text,
            execution_duration_ms,
            user_name,
            warehouse_id,
            start_time,
            rows_read,
            bytes_read
          FROM system.query.history
          WHERE query_id = '{query_id}'
        ),
        similar_executions AS (
          SELECT 
            COUNT(*) as executions_last_30_days,
            AVG(execution_duration_ms) as avg_duration_ms,
            SUM(execution_duration_ms) as total_duration_ms
          FROM system.query.history
          WHERE regexp_replace(statement_text, r'\\b\\d+\\b', 'N') = 
                (SELECT regexp_replace(statement_text, r'\\b\\d+\\b', 'N') FROM query_details)
            AND start_time >= current_date() - INTERVAL 30 DAYS
        )
        SELECT 
          qd.*,
          se.executions_last_30_days,
          se.avg_duration_ms,
          se.total_duration_ms,
          -- Estimate DBU cost (approximate)
          (se.total_duration_ms / 1000.0 / 3600.0) * 0.5 as estimated_monthly_dbu_cost,
          -- Calculate savings
          (se.total_duration_ms * {proposed_improvement_percent / 100.0}) as time_savings_ms,
          ((se.total_duration_ms / 1000.0 / 3600.0) * 0.5 * {proposed_improvement_percent / 100.0}) as monthly_cost_savings
        FROM query_details qd
        CROSS JOIN similar_executions se
        """
        
        results = {
            "query": roi_query,
            "description": f"ROI analysis for optimizing query {query_id}",
            "assumptions": {
                "improvement_percent": proposed_improvement_percent,
                "dbu_cost_estimate": "$0.50 per DBU hour (approximate)",
                "analysis_period": "30 days historical data"
            },
            "roi_metrics": {
                "execution_frequency": "How often this query pattern runs",
                "current_performance": "Baseline execution time and cost",
                "projected_savings": "Time and cost savings from optimization",
                "annual_projection": "Multiply monthly savings by 12"
            },
            "implementation_priority": [
                "High ROI queries should be optimized first",
                "Consider implementation effort vs. savings",
                "Factor in query criticality and user impact",
                "Monitor results to validate improvement assumptions"
            ]
        }
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to calculate ROI: {str(e)}"})

return calculate_optimization_roi(query_id, proposed_improvement_percent)
$$;

-- =============================================================================
-- 6. Grant permissions and setup instructions
-- =============================================================================

-- Grant EXECUTE permissions to users who should access these tools
-- GRANT EXECUTE ON FUNCTION mcp.query_optimization.get_slowest_queries TO `your-group-or-user`;
-- GRANT EXECUTE ON FUNCTION mcp.query_optimization.get_expensive_queries TO `your-group-or-user`;
-- GRANT EXECUTE ON FUNCTION mcp.query_optimization.analyze_query_patterns TO `your-group-or-user`;
-- GRANT EXECUTE ON FUNCTION mcp.query_optimization.get_performance_baseline TO `your-group-or-user`;
-- GRANT EXECUTE ON FUNCTION mcp.query_optimization.calculate_optimization_roi TO `your-group-or-user`;

-- =============================================================================
-- Usage Examples for Agent Playground:
-- =============================================================================

/*
In the Agent Playground, these functions will appear as tools that Claude can use:

Example interactions:
1. "What are my slowest queries in the last 6 hours?"
   → Uses get_slowest_queries(6, 10)

2. "Show me the most expensive queries and calculate ROI for optimizing them"
   → Uses get_expensive_queries() followed by calculate_optimization_roi()

3. "Analyze my query patterns and suggest materialized view opportunities"
   → Uses analyze_query_patterns() 

4. "What's the performance baseline for this query over the last month?"
   → Uses get_performance_baseline(query_text, 30)

The agent will automatically call these functions based on user questions and 
provide intelligent analysis and recommendations using the returned data.
*/