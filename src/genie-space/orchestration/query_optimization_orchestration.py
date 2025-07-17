# Databricks Query Performance Optimization - Orchestration Notebook
# This notebook orchestrates the execution of query performance optimization pipeline

# MAGIC %md
# MAGIC # Databricks Query Performance Optimization - Orchestration
# MAGIC 
# MAGIC This notebook orchestrates the complete query performance optimization pipeline including:
# MAGIC 
# MAGIC ## Pipeline Components
# MAGIC 1. **Schema Setup** - Create schemas and configure permissions
# MAGIC 2. **Core Tables** - Create Delta tables for historical data storage
# MAGIC 3. **Materialized Views** - Create pre-computed aggregations for dashboards
# MAGIC 4. **Regular Views** - Create real-time analysis and alerting views
# MAGIC 5. **ETL Pipeline** - Process incremental data from system tables
# MAGIC 6. **Data Validation** - Verify data quality and completeness
# MAGIC 
# MAGIC ## Execution Schedule
# MAGIC - **Frequency**: Hourly for incremental processing
# MAGIC - **Baseline Updates**: Weekly on Sundays
# MAGIC - **Full Refresh**: Monthly on first Sunday
# MAGIC - **Expected Runtime**: 5-15 minutes
# MAGIC 
# MAGIC ## Configuration
# MAGIC All settings are managed in `../config.yaml`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import yaml
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration
def load_config():
    """Load configuration from YAML file"""
    try:
        with open("../config.yaml", "r") as file:
            config = yaml.safe_load(file)
        logger.info("Configuration loaded successfully")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        # Fallback configuration
        return {
            "schemas": {
                "query_optimization": {"catalog": "mcp", "schema": "query_optimization"}
            },
            "etl": {
                "max_retry_attempts": 3,
                "retry_delay_seconds": 30,
                "incremental_window_hours": 1
            },
            "performance_thresholds": {
                "slow_query_warning_ms": 300000
            }
        }

# Global configuration
CONFIG = load_config()

# Extract key configuration values
CATALOG = CONFIG["schemas"]["query_optimization"]["catalog"]
SCHEMA = CONFIG["schemas"]["query_optimization"]["schema"]
MAX_RETRY_ATTEMPTS = CONFIG["etl"]["max_retry_attempts"]
RETRY_DELAY_SECONDS = CONFIG["etl"]["retry_delay_seconds"]
INCREMENTAL_WINDOW_HOURS = CONFIG["etl"]["incremental_window_hours"]

logger.info(f"Orchestration configured for {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def execute_sql_script(script_path: str, description: str = "", max_retries: int = None) -> Dict:
    """Execute a SQL script with retry logic and comprehensive error handling"""
    
    if max_retries is None:
        max_retries = MAX_RETRY_ATTEMPTS
    
    result = {
        "script": script_path,
        "description": description,
        "status": "PENDING",
        "start_time": datetime.now(),
        "end_time": None,
        "execution_time_seconds": 0,
        "attempt_count": 0,
        "error_message": None,
        "row_count": 0
    }
    
    for attempt in range(max_retries):
        result["attempt_count"] = attempt + 1
        
        try:
            logger.info(f"Executing {script_path} - {description} (attempt {attempt + 1}/{max_retries})")
            
            # Read and execute SQL script
            with open(script_path, 'r') as file:
                sql_content = file.read()
            
            # Execute SQL with timeout
            sql_result = spark.sql(sql_content)
            
            # Collect results if any
            if sql_result:
                rows = sql_result.collect()
                result["row_count"] = len(rows)
            
            # Mark as successful
            result["status"] = "SUCCESS"
            result["end_time"] = datetime.now()
            result["execution_time_seconds"] = (result["end_time"] - result["start_time"]).total_seconds()
            
            logger.info(f"✓ Successfully executed {script_path} in {result['execution_time_seconds']:.2f} seconds")
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"✗ Error executing {script_path} on attempt {attempt + 1}: {error_msg}")
            
            result["error_message"] = error_msg
            
            if attempt == max_retries - 1:
                # Final attempt failed
                result["status"] = "FAILED"
                result["end_time"] = datetime.now()
                result["execution_time_seconds"] = (result["end_time"] - result["start_time"]).total_seconds()
                
                logger.error(f"✗ Failed to execute {script_path} after {max_retries} attempts")
                return result
            
            # Wait before retrying (exponential backoff)
            wait_time = RETRY_DELAY_SECONDS * (2 ** attempt)
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    return result

def execute_sql_command(sql_command: str, description: str = "", max_retries: int = None) -> Dict:
    """Execute a SQL command directly with retry logic"""
    
    if max_retries is None:
        max_retries = MAX_RETRY_ATTEMPTS
    
    result = {
        "command": sql_command[:100] + "..." if len(sql_command) > 100 else sql_command,
        "description": description,
        "status": "PENDING",
        "start_time": datetime.now(),
        "end_time": None,
        "execution_time_seconds": 0,
        "attempt_count": 0,
        "error_message": None,
        "row_count": 0
    }
    
    for attempt in range(max_retries):
        result["attempt_count"] = attempt + 1
        
        try:
            logger.info(f"Executing SQL: {description} (attempt {attempt + 1}/{max_retries})")
            
            # Execute SQL command
            sql_result = spark.sql(sql_command)
            
            # Collect results if any
            if sql_result:
                rows = sql_result.collect()
                result["row_count"] = len(rows)
            
            # Mark as successful
            result["status"] = "SUCCESS"
            result["end_time"] = datetime.now()
            result["execution_time_seconds"] = (result["end_time"] - result["start_time"]).total_seconds()
            
            logger.info(f"✓ Successfully executed SQL command in {result['execution_time_seconds']:.2f} seconds")
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"✗ Error executing SQL command on attempt {attempt + 1}: {error_msg}")
            
            result["error_message"] = error_msg
            
            if attempt == max_retries - 1:
                # Final attempt failed
                result["status"] = "FAILED"
                result["end_time"] = datetime.now()
                result["execution_time_seconds"] = (result["end_time"] - result["start_time"]).total_seconds()
                
                logger.error(f"✗ Failed to execute SQL command after {max_retries} attempts")
                return result
            
            # Wait before retrying
            wait_time = RETRY_DELAY_SECONDS * (2 ** attempt)
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    return result

def check_system_table_access() -> List[Dict]:
    """Verify access to all required system tables"""
    
    required_tables = CONFIG.get("system_tables", {}).get("required_tables", [
        "system.query.history",
        "system.billing.usage", 
        "system.billing.list_prices",
        "system.compute.clusters",
        "system.compute.warehouses"
    ])
    
    access_results = []
    
    for table in required_tables:
        try:
            # Test read access with a simple query
            spark.sql(f"SELECT COUNT(*) FROM {table} LIMIT 1").collect()
            
            access_results.append({
                "table": table,
                "status": "SUCCESS",
                "accessible": True,
                "error": None
            })
            
            logger.info(f"✓ Access confirmed for {table}")
            
        except Exception as e:
            access_results.append({
                "table": table,
                "status": "FAILED", 
                "accessible": False,
                "error": str(e)
            })
            
            logger.error(f"✗ Access denied for {table}: {str(e)}")
    
    return access_results

def check_table_health(table_name: str, schema_name: str = None) -> Dict:
    """Check the health and data quality of a table"""
    
    if schema_name is None:
        schema_name = SCHEMA
    
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"
    
    try:
        # Check if table exists
        spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
        
        # Get basic statistics
        stats_query = f"""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT query_date) as unique_dates,
                MIN(query_date) as earliest_date,
                MAX(query_date) as latest_date,
                MAX(created_at) as latest_created_at
            FROM {full_table_name}
            WHERE query_date >= CURRENT_DATE - INTERVAL 7 DAY
        """
        
        stats = spark.sql(stats_query).collect()[0]
        
        return {
            "table_name": table_name,
            "full_table_name": full_table_name,
            "status": "SUCCESS",
            "exists": True,
            "row_count": stats["row_count"],
            "unique_dates": stats["unique_dates"],
            "earliest_date": stats["earliest_date"],
            "latest_date": stats["latest_date"],
            "latest_created_at": stats["latest_created_at"],
            "data_freshness_hours": None if not stats["latest_created_at"] else 
                (datetime.now() - stats["latest_created_at"]).total_seconds() / 3600,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Error checking table health for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "full_table_name": full_table_name,
            "status": "FAILED",
            "exists": False,
            "error": str(e)
        }

def send_notification(message: str, severity: str = "INFO", details: Dict = None):
    """Send notification about job status"""
    
    timestamp = datetime.now().isoformat()
    
    notification = {
        "timestamp": timestamp,
        "severity": severity,
        "message": message,
        "job": "query_optimization_orchestration",
        "details": details or {}
    }
    
    # Log notification
    if severity == "ERROR":
        logger.error(f"NOTIFICATION: {message}")
    elif severity == "WARNING":
        logger.warning(f"NOTIFICATION: {message}")
    else:
        logger.info(f"NOTIFICATION: {message}")
    
    # TODO: Implement actual notification sending based on config
    # Example implementations:
    # if CONFIG.get("notifications", {}).get("channels", {}).get("slack", {}).get("enabled"):
    #     send_slack_notification(notification)
    # if CONFIG.get("notifications", {}).get("channels", {}).get("email", {}).get("enabled"):
    #     send_email_notification(notification)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-execution Validation

# COMMAND ----------

def validate_prerequisites() -> Tuple[bool, List[str]]:
    """Validate all prerequisites before starting the pipeline"""
    
    logger.info("Starting prerequisite validation...")
    validation_errors = []
    
    # 1. Check system table access
    logger.info("Checking system table access...")
    access_results = check_system_table_access()
    
    failed_access = [r for r in access_results if not r["accessible"]]
    if failed_access:
        for failure in failed_access:
            validation_errors.append(f"Cannot access {failure['table']}: {failure['error']}")
    
    # 2. Check schema exists
    try:
        spark.sql(f"USE CATALOG {CATALOG}")
        spark.sql(f"SHOW SCHEMAS LIKE '{SCHEMA}'").collect()
        logger.info(f"✓ Schema {CATALOG}.{SCHEMA} exists")
    except Exception as e:
        validation_errors.append(f"Schema {CATALOG}.{SCHEMA} not accessible: {str(e)}")
    
    # 3. Check configuration validity
    required_config_keys = ["schemas", "etl", "performance_thresholds"]
    for key in required_config_keys:
        if key not in CONFIG:
            validation_errors.append(f"Missing required configuration section: {key}")
    
    # 4. Check available resources
    try:
        # Simple query to test cluster resources
        spark.sql("SELECT 1").collect()
        logger.info("✓ Cluster resources available")
    except Exception as e:
        validation_errors.append(f"Cluster resource issue: {str(e)}")
    
    success = len(validation_errors) == 0
    
    if success:
        logger.info("✓ All prerequisites validated successfully")
    else:
        logger.error(f"✗ Validation failed with {len(validation_errors)} errors")
        for error in validation_errors:
            logger.error(f"  - {error}")
    
    return success, validation_errors

# Run prerequisite validation
validation_success, validation_errors = validate_prerequisites()

if not validation_success:
    error_message = f"Prerequisites validation failed: {'; '.join(validation_errors)}"
    send_notification(error_message, "ERROR")
    raise Exception(error_message)

logger.info("Prerequisites validation completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline Components

# COMMAND ----------

def execute_pipeline() -> Dict:
    """Execute the complete query optimization pipeline"""
    
    pipeline_start_time = datetime.now()
    pipeline_results = []
    
    logger.info(f"Starting query optimization pipeline at {pipeline_start_time}")
    send_notification("Starting query optimization pipeline execution")
    
    # Define pipeline steps
    pipeline_steps = [
        {
            "script": "../sql/01_schema_setup.sql",
            "description": "Setup schema and permissions",
            "required": True
        },
        {
            "script": "../sql/02_core_tables.sql", 
            "description": "Create core Delta tables",
            "required": True
        },
        {
            "script": "../sql/03_materialized_views.sql",
            "description": "Create materialized views for dashboards",
            "required": True
        },
        {
            "script": "../sql/04_regular_views.sql",
            "description": "Create real-time analysis views", 
            "required": True
        }
    ]
    
    # Execute each pipeline step
    for step in pipeline_steps:
        logger.info(f"\n--- Executing: {step['description']} ---")
        
        result = execute_sql_script(
            script_path=step["script"],
            description=step["description"]
        )
        
        pipeline_results.append(result)
        
        # Check if required step failed
        if step["required"] and result["status"] == "FAILED":
            error_msg = f"Required pipeline step failed: {step['description']}"
            logger.error(error_msg)
            send_notification(error_msg, "ERROR", {"step": step, "result": result})
            
            # Return early with failure status
            return {
                "status": "FAILED",
                "start_time": pipeline_start_time,
                "end_time": datetime.now(),
                "total_execution_time": (datetime.now() - pipeline_start_time).total_seconds(),
                "steps_completed": len(pipeline_results),
                "steps_total": len(pipeline_steps),
                "results": pipeline_results,
                "error": error_msg
            }
    
    # Execute ETL pipeline for incremental data processing
    logger.info("\n--- Executing: ETL Pipeline ---")
    
    # Calculate time window for incremental processing
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=INCREMENTAL_WINDOW_HOURS)
    
    etl_command = f"""
    CALL mcp.query_optimization.process_query_performance_incremental(
        TIMESTAMP('{start_time.isoformat()}'),
        TIMESTAMP('{end_time.isoformat()}')
    )
    """
    
    etl_result = execute_sql_command(
        sql_command=etl_command,
        description="Process incremental query performance data"
    )
    
    pipeline_results.append(etl_result)
    
    # Calculate final results
    pipeline_end_time = datetime.now()
    total_execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
    
    successful_steps = len([r for r in pipeline_results if r["status"] == "SUCCESS"])
    failed_steps = len([r for r in pipeline_results if r["status"] == "FAILED"])
    
    overall_status = "SUCCESS" if failed_steps == 0 else "PARTIAL_SUCCESS" if successful_steps > 0 else "FAILED"
    
    pipeline_summary = {
        "status": overall_status,
        "start_time": pipeline_start_time,
        "end_time": pipeline_end_time,
        "total_execution_time": total_execution_time,
        "steps_completed": successful_steps,
        "steps_failed": failed_steps,
        "steps_total": len(pipeline_results),
        "results": pipeline_results
    }
    
    logger.info(f"\nPipeline completed with status: {overall_status}")
    logger.info(f"Total execution time: {total_execution_time:.2f} seconds")
    logger.info(f"Steps: {successful_steps} successful, {failed_steps} failed, {len(pipeline_results)} total")
    
    return pipeline_summary

# Execute the pipeline
pipeline_summary = execute_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-execution Validation and Health Checks

# COMMAND ----------

def validate_pipeline_results() -> Dict:
    """Validate pipeline results and check data quality"""
    
    logger.info("Starting post-execution validation...")
    
    validation_results = {
        "validation_start_time": datetime.now(),
        "table_health_checks": [],
        "data_quality_checks": [],
        "overall_status": "PENDING"
    }
    
    # Check health of key tables
    key_tables = [
        "query_performance_raw",
        "query_patterns", 
        "optimization_tracking",
        "performance_baselines"
    ]
    
    for table_name in key_tables:
        health_check = check_table_health(table_name)
        validation_results["table_health_checks"].append(health_check)
        
        if health_check["status"] == "SUCCESS":
            logger.info(f"✓ {table_name}: {health_check['row_count']} rows, latest: {health_check['latest_date']}")
        else:
            logger.error(f"✗ {table_name}: Health check failed")
    
    # Run data quality validation script
    try:
        logger.info("Running data quality validation...")
        
        quality_result = execute_sql_script(
            script_path="../validation/data_quality_tests.sql",
            description="Data quality validation tests"
        )
        
        validation_results["data_quality_checks"].append(quality_result)
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        validation_results["data_quality_checks"].append({
            "status": "FAILED",
            "error": str(e)
        })
    
    # Determine overall validation status
    health_failures = [h for h in validation_results["table_health_checks"] if h["status"] == "FAILED"]
    quality_failures = [q for q in validation_results["data_quality_checks"] if q["status"] == "FAILED"]
    
    if health_failures or quality_failures:
        validation_results["overall_status"] = "FAILED"
        logger.error(f"Validation failed: {len(health_failures)} table health failures, {len(quality_failures)} quality failures")
    else:
        validation_results["overall_status"] = "SUCCESS"
        logger.info("✓ All post-execution validations passed")
    
    validation_results["validation_end_time"] = datetime.now()
    
    return validation_results

# Run post-execution validation
validation_results = validate_pipeline_results()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Summary Report and Notifications

# COMMAND ----------

def generate_summary_report(pipeline_summary: Dict, validation_results: Dict) -> Dict:
    """Generate comprehensive summary report"""
    
    report = {
        "execution_timestamp": datetime.now().isoformat(),
        "pipeline_summary": pipeline_summary,
        "validation_results": validation_results,
        "configuration": {
            "catalog": CATALOG,
            "schema": SCHEMA,
            "incremental_window_hours": INCREMENTAL_WINDOW_HOURS,
            "config_version": CONFIG.get("version_info", {}).get("config_version", "unknown")
        },
        "performance_metrics": {
            "total_execution_time_seconds": pipeline_summary.get("total_execution_time", 0),
            "avg_step_time_seconds": pipeline_summary.get("total_execution_time", 0) / max(1, pipeline_summary.get("steps_total", 1)),
            "success_rate": pipeline_summary.get("steps_completed", 0) / max(1, pipeline_summary.get("steps_total", 1))
        }
    }
    
    # Determine overall status
    pipeline_status = pipeline_summary.get("status", "UNKNOWN")
    validation_status = validation_results.get("overall_status", "UNKNOWN")
    
    if pipeline_status == "SUCCESS" and validation_status == "SUCCESS":
        report["overall_status"] = "SUCCESS"
        severity = "INFO"
        message = f"Query optimization pipeline completed successfully in {report['performance_metrics']['total_execution_time_seconds']:.2f} seconds"
    elif pipeline_status == "PARTIAL_SUCCESS" or validation_status == "FAILED":
        report["overall_status"] = "WARNING"
        severity = "WARNING"
        message = f"Query optimization pipeline completed with warnings. Pipeline: {pipeline_status}, Validation: {validation_status}"
    else:
        report["overall_status"] = "FAILED"
        severity = "ERROR"
        message = f"Query optimization pipeline failed. Pipeline: {pipeline_status}, Validation: {validation_status}"
    
    # Send notification
    send_notification(message, severity, {
        "execution_time": report["performance_metrics"]["total_execution_time_seconds"],
        "success_rate": report["performance_metrics"]["success_rate"],
        "steps_completed": pipeline_summary.get("steps_completed", 0),
        "steps_total": pipeline_summary.get("steps_total", 0)
    })
    
    return report

# Generate summary report
summary_report = generate_summary_report(pipeline_summary, validation_results)

# Display summary
logger.info("\n" + "="*50)
logger.info("EXECUTION SUMMARY")
logger.info("="*50)
logger.info(f"Overall Status: {summary_report['overall_status']}")
logger.info(f"Total Execution Time: {summary_report['performance_metrics']['total_execution_time_seconds']:.2f} seconds")
logger.info(f"Success Rate: {summary_report['performance_metrics']['success_rate']:.2%}")
logger.info(f"Steps Completed: {pipeline_summary.get('steps_completed', 0)}/{pipeline_summary.get('steps_total', 0)}")

# Display as DataFrame for better visibility in Databricks
display(spark.createDataFrame([summary_report["performance_metrics"]]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Optimization

# COMMAND ----------

def perform_table_optimization():
    """Optimize tables for better performance"""
    
    logger.info("Starting table optimization...")
    
    tables_to_optimize = [
        "query_performance_raw",
        "query_patterns",
        "optimization_tracking", 
        "performance_baselines"
    ]
    
    optimization_results = []
    
    for table_name in tables_to_optimize:
        try:
            full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
            
            # Run OPTIMIZE command
            optimize_start = datetime.now()
            spark.sql(f"OPTIMIZE {full_table_name}")
            optimize_end = datetime.now()
            
            # Update table statistics
            spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS")
            
            optimization_results.append({
                "table": table_name,
                "status": "SUCCESS",
                "optimization_time_seconds": (optimize_end - optimize_start).total_seconds()
            })
            
            logger.info(f"✓ Optimized {table_name}")
            
        except Exception as e:
            optimization_results.append({
                "table": table_name,
                "status": "FAILED",
                "error": str(e)
            })
            
            logger.error(f"✗ Failed to optimize {table_name}: {str(e)}")
    
    # Refresh materialized views
    try:
        materialized_views = [
            "mv_hourly_performance",
            "mv_daily_performance", 
            "mv_pattern_performance",
            "mv_user_performance",
            "mv_performance_alerts"
        ]
        
        for view_name in materialized_views:
            spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.{SCHEMA}.{view_name}")
            logger.info(f"✓ Refreshed materialized view {view_name}")
    
    except Exception as e:
        logger.error(f"Error refreshing materialized views: {str(e)}")
    
    logger.info("Table optimization completed")
    return optimization_results

# Perform optimization
optimization_results = perform_table_optimization()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Status and Cleanup

# COMMAND ----------

# Store execution log
execution_log = {
    "execution_date": datetime.now().date(),
    "execution_timestamp": datetime.now().isoformat(),
    "overall_status": summary_report["overall_status"],
    "execution_time_seconds": summary_report["performance_metrics"]["total_execution_time_seconds"],
    "pipeline_summary": pipeline_summary,
    "validation_results": validation_results,
    "optimization_results": optimization_results,
    "config_version": CONFIG.get("version_info", {}).get("config_version", "unknown")
}

# TODO: Store execution log in tracking table
# spark.createDataFrame([execution_log]).write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.orchestration_log")

# Final logging
logger.info(f"\n🎯 Query Optimization Orchestration completed with status: {summary_report['overall_status']}")
logger.info(f"📊 Total execution time: {summary_report['performance_metrics']['total_execution_time_seconds']:.2f} seconds")
logger.info(f"✅ Success rate: {summary_report['performance_metrics']['success_rate']:.2%}")

if summary_report["overall_status"] == "SUCCESS":
    logger.info("🌟 All systems operational - query optimization pipeline running successfully!")
elif summary_report["overall_status"] == "WARNING":
    logger.warning("⚠️  Pipeline completed with warnings - review validation results")
else:
    logger.error("❌ Pipeline execution failed - immediate attention required")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling Configuration
# MAGIC 
# MAGIC ### Recommended Schedule
# MAGIC 
# MAGIC 1. **Hourly Incremental Processing**:
# MAGIC    - Schedule: `0 * * * *` (every hour)
# MAGIC    - Purpose: Process new query data incrementally
# MAGIC    - Expected runtime: 5-10 minutes
# MAGIC 
# MAGIC 2. **Daily Full Validation**:
# MAGIC    - Schedule: `0 6 * * *` (daily at 6 AM UTC)
# MAGIC    - Purpose: Full data validation and health checks
# MAGIC    - Expected runtime: 10-15 minutes
# MAGIC 
# MAGIC 3. **Weekly Baseline Updates**:
# MAGIC    - Schedule: `0 2 * * 0` (Sunday at 2 AM UTC)
# MAGIC    - Purpose: Update performance baselines
# MAGIC    - Expected runtime: 15-30 minutes
# MAGIC 
# MAGIC ### Job Configuration Steps
# MAGIC 
# MAGIC 1. **Create Databricks Job**:
# MAGIC    - Go to Workflows → Jobs → Create Job
# MAGIC    - Task Type: Notebook
# MAGIC    - Notebook Path: This notebook
# MAGIC    - Cluster: Use job cluster or existing cluster
# MAGIC 
# MAGIC 2. **Configure Parameters**:
# MAGIC    - Add parameter `execution_mode` with values: `incremental`, `full`, `baseline_update`
# MAGIC    - Add parameter `config_path` pointing to config.yaml location
# MAGIC 
# MAGIC 3. **Set Up Alerts**:
# MAGIC    - Email notifications on failure
# MAGIC    - Slack/Teams integration if configured
# MAGIC    - Alert on long-running jobs (>30 minutes)
# MAGIC 
# MAGIC 4. **Monitor Performance**:
# MAGIC    - Review execution logs in job runs
# MAGIC    - Monitor resource usage and optimize cluster size
# MAGIC    - Track data processing volumes and adjust accordingly