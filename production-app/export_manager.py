"""
Export Manager - Turn optimizations into downloadable scripts!
The final piece of our optimization beast! 📦
"""

import zipfile
import io
from datetime import datetime
import streamlit as st

class ResultsExportManager:
    """
    Handles exporting optimization results, test scripts, and complete packages
    Making it easy to test and implement optimizations! 🚀
    """
    
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def generate_test_script(self, original_query, optimized_query, query_description="Query Optimization Test"):
        """
        Generate an A/B test script to compare original vs optimized performance
        This is where we prove the optimization works! 🧪
        """
        
        test_script = f"""-- ========================================
-- Query Optimization A/B Test Script
-- Generated: {datetime.now().isoformat()}
-- Description: {query_description}
-- ========================================

-- INSTRUCTIONS:
-- 1. Run this script in your Databricks SQL workspace
-- 2. Compare execution times between original and optimized queries
-- 3. Verify results are identical between both versions
-- 4. Monitor resource usage in query history

SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;

-- ========================================
-- TEST 1: ORIGINAL QUERY PERFORMANCE
-- ========================================

SELECT 'PERFORMANCE_TEST_ORIGINAL' as test_marker, current_timestamp() as test_start_time;

-- Original Query (Baseline)
{self._add_sql_comments(original_query, "ORIGINAL QUERY")}

SELECT 'PERFORMANCE_TEST_ORIGINAL' as test_marker, current_timestamp() as test_end_time;

-- ========================================
-- TEST 2: OPTIMIZED QUERY PERFORMANCE  
-- ========================================

SELECT 'PERFORMANCE_TEST_OPTIMIZED' as test_marker, current_timestamp() as test_start_time;

-- Optimized Query (Improved Version)
{self._add_sql_comments(optimized_query, "OPTIMIZED QUERY")}

SELECT 'PERFORMANCE_TEST_OPTIMIZED' as test_marker, current_timestamp() as test_end_time;

-- ========================================
-- TEST 3: PERFORMANCE COMPARISON
-- ========================================

-- Check query execution history for performance comparison
-- Run this after both queries complete
SELECT 
    CASE 
        WHEN statement_text LIKE '%PERFORMANCE_TEST_ORIGINAL%' THEN 'ORIGINAL'
        WHEN statement_text LIKE '%PERFORMANCE_TEST_OPTIMIZED%' THEN 'OPTIMIZED'
        ELSE 'OTHER'
    END as query_type,
    execution_duration_ms,
    total_duration_ms,
    rows_read,
    bytes_read,
    executed_as_user_name,
    start_time,
    end_time
FROM system.query.history 
WHERE start_time >= current_timestamp() - INTERVAL 2 HOURS
    AND (statement_text LIKE '%PERFORMANCE_TEST_ORIGINAL%' 
         OR statement_text LIKE '%PERFORMANCE_TEST_OPTIMIZED%')
    AND statement_type = 'SELECT'
ORDER BY start_time DESC;

-- ========================================
-- TEST 4: RESULTS VALIDATION
-- ========================================

-- TODO: Add specific validation queries to ensure identical results
-- Example:
-- WITH original_results AS (
--     {original_query.replace(';', '')}
-- ),
-- optimized_results AS (
--     {optimized_query.replace(';', '')}
-- )
-- SELECT 
--     COUNT(*) as original_count,
--     (SELECT COUNT(*) FROM optimized_results) as optimized_count,
--     CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM optimized_results) 
--          THEN '✅ RESULTS MATCH' 
--          ELSE '❌ RESULTS DIFFER' 
--     END as validation_status
-- FROM original_results;

-- ========================================
-- PERFORMANCE SUMMARY
-- ========================================

-- Manual calculation template:
-- Original Time: ___ seconds
-- Optimized Time: ___ seconds  
-- Improvement: ___% faster
-- Original Cost: ___ DBUs
-- Optimized Cost: ___ DBUs
-- Savings: ___% cost reduction

"""
        return test_script
    
    def generate_ddl_script(self, recommendations):
        """Generate DDL statements for recommended optimizations"""
        
        ddl_script = f"""-- ========================================
-- DDL Script for Query Optimizations
-- Generated: {datetime.now().isoformat()}
-- ========================================

-- INSTRUCTIONS:
-- 1. Review each DDL statement carefully
-- 2. Test in development environment first
-- 3. Consider impact on existing queries
-- 4. Monitor performance after implementation

-- ========================================
-- RECOMMENDED INDEXES
-- ========================================

-- TODO: Customize these based on actual recommendations
-- Example index creation (MODIFY FOR YOUR TABLES):

-- CREATE INDEX idx_customer_date ON customers (customer_id, created_date);
-- CREATE INDEX idx_orders_customer ON orders (customer_id, order_date);

-- ========================================
-- RECOMMENDED PARTITIONING
-- ========================================

-- TODO: Add table partitioning commands if recommended
-- Example partitioning (MODIFY FOR YOUR TABLES):

-- ALTER TABLE orders CLUSTER BY (order_date);
-- ALTER TABLE customers CLUSTER BY (region);

-- ========================================
-- RECOMMENDED MATERIALIZED VIEWS
-- ========================================

-- TODO: Add materialized view creation if recommended
-- Example materialized view (MODIFY FOR YOUR USE CASE):

-- CREATE MATERIALIZED VIEW mv_customer_summary AS
-- SELECT 
--     customer_id,
--     COUNT(*) as order_count,
--     SUM(total_amount) as total_spent,
--     MAX(order_date) as last_order_date
-- FROM orders 
-- GROUP BY customer_id;

-- ========================================
-- RECOMMENDED TABLE OPTIMIZATIONS
-- ========================================

-- TODO: Add table optimization commands
-- Examples:

-- OPTIMIZE customers;
-- OPTIMIZE orders;
-- ANALYZE TABLE customers COMPUTE STATISTICS;
-- ANALYZE TABLE orders COMPUTE STATISTICS;

-- ========================================
-- NOTES
-- ========================================

-- Recommendations from AI analysis:
{chr(10).join(f"-- • {rec}" for rec in recommendations) if recommendations else "-- No specific DDL recommendations provided"}

"""
        return ddl_script
    
    def generate_rollback_script(self, original_query):
        """Generate rollback script in case optimization causes issues"""
        
        rollback_script = f"""-- ========================================
-- ROLLBACK SCRIPT
-- Generated: {datetime.now().isoformat()}
-- ========================================

-- Use this script if the optimization causes issues
-- and you need to revert to the original query

-- ========================================
-- ORIGINAL QUERY (BACKUP)
-- ========================================

{self._add_sql_comments(original_query, "ORIGINAL QUERY - USE IF ROLLBACK NEEDED")}

-- ========================================
-- ROLLBACK CHECKLIST
-- ========================================

-- □ 1. Remove any new indexes created for optimization
-- □ 2. Drop any materialized views created
-- □ 3. Revert any table partitioning changes
-- □ 4. Update application code to use original query
-- □ 5. Monitor performance to ensure stability

-- ========================================
-- DDL ROLLBACK COMMANDS
-- ========================================

-- TODO: Add specific rollback commands based on what was implemented
-- Examples:

-- DROP INDEX IF EXISTS idx_customer_date;
-- DROP MATERIALIZED VIEW IF EXISTS mv_customer_summary;

"""
        return rollback_script
    
    def create_optimization_report(self, query_id, analysis_result, optimization_result):
        """Create a comprehensive optimization report"""
        
        report = f"""# Query Optimization Report

**Generated:** {datetime.now().isoformat()}
**Query ID:** {query_id}
**Analyzer:** {optimization_result.get('model_used', 'Llama 70B')}

## Executive Summary

This report contains the analysis and optimization recommendations for the selected query. The AI analysis identified several performance improvement opportunities with estimated impact on execution time and cost.

## Performance Analysis

{analysis_result.get('analysis', 'Analysis not available')}

## Key Recommendations

{chr(10).join(f"- {rec}" for rec in analysis_result.get('recommendations', [])) if analysis_result.get('recommendations') else "No specific recommendations available"}

## Optimized Query

The following optimized query implements the recommended improvements:

```sql
{optimization_result.get('optimized_query', 'Optimized query not available')}
```

## Changes Made

{chr(10).join(f"- {change}" for change in optimization_result.get('changes_made', [])) if optimization_result.get('changes_made') else "No changes documented"}

## Implementation Guide

1. **Test First**: Use the provided A/B test script to validate performance improvements
2. **Backup Original**: Keep the original query as a rollback option
3. **Monitor Performance**: Track execution times and resource usage after implementation
4. **Validate Results**: Ensure the optimized query produces identical results

## Files Included

- `optimized_query.sql` - The optimized query ready for implementation
- `performance_test.sql` - A/B test script to validate improvements  
- `ddl_changes.sql` - Required database changes (indexes, etc.)
- `rollback_script.sql` - Rollback to original query if needed
- `analysis_report.md` - This comprehensive report

## Expected Impact

Based on the AI analysis, this optimization is expected to provide:
- **Performance**: Faster execution times
- **Cost Savings**: Reduced DBU consumption
- **Resource Efficiency**: Better use of compute resources

## Support

If you encounter issues with the optimization:
1. Use the rollback script to revert changes
2. Check the performance test results for validation
3. Monitor query execution history for performance trends

---
*Generated by Query Optimization Beast 🚀*
"""
        return report
    
    def create_complete_package(self, query_id, original_query, analysis_result, optimization_result):
        """Create a complete downloadable package with all optimization materials"""
        
        # Create in-memory zip file
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            
            # 1. Optimized Query
            zip_file.writestr(
                "optimized_query.sql",
                optimization_result.get('optimized_query', '-- Optimized query not available')
            )
            
            # 2. Performance Test Script
            zip_file.writestr(
                "performance_test.sql",
                self.generate_test_script(original_query, optimization_result.get('optimized_query', ''))
            )
            
            # 3. DDL Changes
            zip_file.writestr(
                "ddl_changes.sql", 
                self.generate_ddl_script(analysis_result.get('recommendations', []))
            )
            
            # 4. Rollback Script
            zip_file.writestr(
                "rollback_script.sql",
                self.generate_rollback_script(original_query)
            )
            
            # 5. Analysis Report
            zip_file.writestr(
                "analysis_report.md",
                self.create_optimization_report(query_id, analysis_result, optimization_result)
            )
            
            # 6. Original Query (backup)
            zip_file.writestr(
                "original_query.sql",
                self._add_sql_comments(original_query, "ORIGINAL QUERY - BACKUP")
            )
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
    
    def _add_sql_comments(self, query, title):
        """Add helpful comments to SQL query"""
        commented_query = f"""-- ========================================
-- {title}
-- ========================================

{query}

-- ========================================
-- END {title}
-- ========================================
"""
        return commented_query
    
    def get_download_filename(self, query_id, file_type="package"):
        """Generate appropriate filename for downloads"""
        base_name = f"query_optimization_{query_id or 'unknown'}_{self.timestamp}"
        
        extensions = {
            "package": f"{base_name}.zip",
            "test": f"{base_name}_test.sql", 
            "query": f"{base_name}_optimized.sql",
            "report": f"{base_name}_report.md"
        }
        
        return extensions.get(file_type, f"{base_name}.txt")

# Streamlit integration helpers
def create_download_button(export_manager, label, content, filename, file_type="text/plain"):
    """Helper to create download buttons in Streamlit"""
    return st.download_button(
        label=label,
        data=content,
        file_name=filename,
        mime=file_type
    )

def show_export_options(export_manager, query_id, original_query, analysis_result, optimization_result):
    """Show all export options in Streamlit"""
    
    st.subheader("📦 Export Optimization Package")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Individual SQL files
        st.write("**📄 Individual Files**")
        
        # Optimized query
        create_download_button(
            export_manager,
            "📋 Optimized Query",
            optimization_result.get('optimized_query', ''),
            export_manager.get_download_filename(query_id, "query"),
            "text/plain"
        )
        
        # Test script
        test_script = export_manager.generate_test_script(
            original_query, 
            optimization_result.get('optimized_query', '')
        )
        create_download_button(
            export_manager,
            "🧪 Test Script", 
            test_script,
            export_manager.get_download_filename(query_id, "test"),
            "text/plain"
        )
    
    with col2:
        # Support files
        st.write("**🛠️ Support Files**")
        
        # DDL script
        ddl_script = export_manager.generate_ddl_script(
            analysis_result.get('recommendations', [])
        )
        create_download_button(
            export_manager,
            "🗃️ DDL Changes",
            ddl_script,
            f"ddl_changes_{export_manager.timestamp}.sql",
            "text/plain"
        )
        
        # Rollback script
        rollback_script = export_manager.generate_rollback_script(original_query)
        create_download_button(
            export_manager,
            "🔄 Rollback Script",
            rollback_script,
            f"rollback_{export_manager.timestamp}.sql", 
            "text/plain"
        )
    
    with col3:
        # Complete package
        st.write("**📦 Complete Package**")
        
        complete_package = export_manager.create_complete_package(
            query_id, original_query, analysis_result, optimization_result
        )
        
        create_download_button(
            export_manager,
            "🎁 Full Package (ZIP)",
            complete_package,
            export_manager.get_download_filename(query_id, "package"),
            "application/zip"
        )
        
        st.info("📋 **Includes:** Optimized query, test script, DDL changes, rollback script, and analysis report")

# Test function
def test_export_manager():
    """Test the export manager functionality"""
    print("🧪 Testing Export Manager...")
    
    export_manager = ResultsExportManager()
    
    # Test data
    test_query = "SELECT * FROM customers WHERE region = 'US';"
    optimized_query = "SELECT * FROM customers WHERE region = 'US' AND created_date >= '2023-01-01';"
    
    # Test script generation
    test_script = export_manager.generate_test_script(test_query, optimized_query)
    print(f"✅ Test script generated: {len(test_script)} characters")
    
    # Test complete package
    mock_analysis = {"analysis": "Test analysis", "recommendations": ["Add index", "Optimize JOIN"]}
    mock_optimization = {"optimized_query": optimized_query, "changes_made": ["Added index"], "model_used": "Llama 70B"}
    
    package = export_manager.create_complete_package("test_123", test_query, mock_analysis, mock_optimization)
    print(f"✅ Complete package generated: {len(package)} bytes")
    
    print("🎉 Export Manager Test Complete!")
    return True

if __name__ == "__main__":
    test_export_manager()