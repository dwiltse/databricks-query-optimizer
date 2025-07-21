# Databricks notebook source
# MAGIC %md
# MAGIC # Query Performance Tables Refresh
# MAGIC 
# MAGIC This notebook refreshes performance tables with incremental data from system tables.
# MAGIC Better than stored procedures: version controlled, testable, configurable, loggable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration (can be parameterized via Databricks Jobs)
config = {
    "catalog": "mcp",
    "schema": "query_optimization", 
    "incremental_hours": 2,  # Process last 2 hours of data
    "batch_size": 10000,
    "enable_logging": True
}

# Set up logging
def log_info(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if config["enable_logging"]:
        print(f"[{timestamp}] INFO: {message}")

def log_error(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}")

log_info("Starting performance tables refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_latest_timestamp(table_name, timestamp_column="end_time"):
    """Get latest timestamp from table for incremental processing"""
    try:
        latest = spark.sql(f"""
            SELECT COALESCE(MAX({timestamp_column}), TIMESTAMP('1900-01-01 00:00:00')) as latest
            FROM {config['catalog']}.{config['schema']}.{table_name}
        """).collect()[0]['latest']
        
        log_info(f"Latest timestamp for {table_name}: {latest}")
        return latest
    except Exception as e:
        log_error(f"Error getting latest timestamp for {table_name}: {str(e)}")
        return datetime(1900, 1, 1)

def validate_data_quality(df, table_name, expected_min_rows=1):
    """Basic data quality validation"""
    row_count = df.count()
    
    if row_count < expected_min_rows:
        log_error(f"{table_name}: Only {row_count} rows processed, expected at least {expected_min_rows}")
        return False
    
    # Check for null values in critical columns
    null_checks = df.select([
        sum(col(c).isNull().cast("int")).alias(f"{c}_nulls") 
        for c in df.columns[:5]  # Check first 5 columns
    ]).collect()[0]
    
    for col_name, null_count in null_checks.asDict().items():
        if null_count is not None and null_count > row_count * 0.1:  # More than 10% nulls
            log_error(f"{table_name}: High null rate in {col_name}: {null_count}/{row_count}")
    
    log_info(f"{table_name}: Data quality check passed - {row_count} rows processed")
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Refresh Query Performance Categorized

# COMMAND ----------

def refresh_query_performance_categorized():
    """Refresh query performance with business logic categorization"""
    log_info("Refreshing query_performance_categorized")
    
    try:
        # Get latest timestamp for incremental processing
        latest_timestamp = get_latest_timestamp("query_performance_categorized")
        
        # Read new data from system tables
        new_data = spark.sql(f"""
            SELECT
                statement_id,
                statement_text,
                executed_by,
                executed_as,
                total_duration_ms,
                execution_duration_ms,
                result_fetch_duration_ms,
                read_bytes,
                read_rows,
                read_partitions,
                error_message,
                compute.warehouse_id as warehouse_id,
                compute.type as compute_type,
                end_time,
                -- Business logic: SLOW/MODERATE/FAST categorization
                CASE
                    WHEN execution_duration_ms > 300000 THEN 'SLOW'
                    WHEN execution_duration_ms > 60000 THEN 'MODERATE'
                    ELSE 'FAST'
                END AS performance_category,
                -- Efficiency metric
                CASE
                    WHEN read_bytes > 0 AND read_rows > 0 THEN CAST(read_bytes AS DOUBLE) / CAST(read_rows AS DOUBLE)
                    ELSE NULL
                END AS bytes_per_row_efficiency,
                -- Optimization flags
                CASE
                    WHEN error_message IS NOT NULL THEN 'ERROR'
                    WHEN result_fetch_duration_ms > 30000 THEN 'SLOW_FETCH'
                    ELSE 'HEALTHY'
                END AS optimization_flag,
                CURRENT_TIMESTAMP() as created_at
            FROM system.query.history
            WHERE end_time > TIMESTAMP('{latest_timestamp}')
                AND end_time >= CURRENT_TIMESTAMP() - INTERVAL {config['incremental_hours']} HOURS
                AND compute.warehouse_id IS NOT NULL
                AND execution_duration_ms IS NOT NULL
        """)
        
        # Data quality validation
        if not validate_data_quality(new_data, "query_performance_categorized", 0):  # 0 min rows OK for incremental
            return False
        
        # Write to table
        if new_data.count() > 0:
            new_data.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"{config['catalog']}.{config['schema']}.query_performance_categorized")
            
            log_info(f"Successfully refreshed query_performance_categorized: {new_data.count()} rows added")
        else:
            log_info("No new data for query_performance_categorized")
        
        return True
        
    except Exception as e:
        log_error(f"Failed to refresh query_performance_categorized: {str(e)}")
        return False

# Execute
success_1 = refresh_query_performance_categorized()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Refresh Current Slow Queries

# COMMAND ----------

def refresh_current_slow_queries():
    """Refresh current slow queries with optimization suggestions"""
    log_info("Refreshing current_slow_queries")
    
    try:
        # Clean old data first (keep last 24 hours)
        spark.sql(f"""
            DELETE FROM {config['catalog']}.{config['schema']}.current_slow_queries 
            WHERE first_seen < CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
        """)
        
        # Get current slow queries
        slow_queries = spark.sql(f"""
            SELECT 
                statement_id,
                executed_by,
                compute.warehouse_id as warehouse_id,
                execution_duration_ms,
                read_bytes,
                -- Performance impact score (1-10)
                CASE 
                    WHEN execution_duration_ms > 1800000 THEN 10  -- 30+ minutes
                    WHEN execution_duration_ms > 900000 THEN 8    -- 15+ minutes  
                    WHEN execution_duration_ms > 300000 THEN 6    -- 5+ minutes
                    ELSE 3
                END as performance_impact_score,
                -- AI-like optimization suggestions based on patterns
                CASE
                    WHEN statement_text LIKE '%SELECT *%' THEN 'Replace SELECT * with specific columns'
                    WHEN statement_text LIKE '%ORDER BY%' AND statement_text NOT LIKE '%LIMIT%' THEN 'Add LIMIT clause to ORDER BY'
                    WHEN CAST(read_bytes AS DOUBLE) / NULLIF(CAST(read_rows AS DOUBLE), 0) > 100000 THEN 'Optimize data access - high bytes per row'
                    WHEN statement_text LIKE '%DISTINCT%' THEN 'Consider if DISTINCT is necessary or use GROUP BY'
                    WHEN statement_text LIKE '% FROM %,%' AND statement_text NOT LIKE '%JOIN%' THEN 'Replace cartesian join with proper JOIN'
                    ELSE 'Review query execution plan and consider indexing'
                END as suggested_optimization,
                end_time as first_seen
            FROM system.query.history
            WHERE execution_duration_ms > 300000  -- SLOW queries only (your business logic)
                AND end_time >= CURRENT_TIMESTAMP() - INTERVAL 4 HOURS
                AND compute.warehouse_id IS NOT NULL
        """)
        
        if validate_data_quality(slow_queries, "current_slow_queries", 0):
            if slow_queries.count() > 0:
                # Use MERGE for upsert logic
                slow_queries.createOrReplaceTempView("new_slow_queries")
                
                spark.sql(f"""
                    MERGE INTO {config['catalog']}.{config['schema']}.current_slow_queries AS target
                    USING new_slow_queries AS source
                    ON target.statement_id = source.statement_id
                    WHEN MATCHED THEN UPDATE SET
                        occurrence_count = target.occurrence_count + 1,
                        created_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        statement_id, executed_by, warehouse_id, execution_duration_ms, read_bytes,
                        performance_impact_score, suggested_optimization, first_seen, occurrence_count, created_at
                    ) VALUES (
                        source.statement_id, source.executed_by, source.warehouse_id, source.execution_duration_ms, 
                        source.read_bytes, source.performance_impact_score, source.suggested_optimization, 
                        source.first_seen, 1, CURRENT_TIMESTAMP()
                    )
                """)
                
                log_info(f"Successfully refreshed current_slow_queries: {slow_queries.count()} queries processed")
            else:
                log_info("No slow queries found - good performance!")
            
            return True
        else:
            return False
            
    except Exception as e:
        log_error(f"Failed to refresh current_slow_queries: {str(e)}")
        return False

# Execute
success_2 = refresh_current_slow_queries()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Refresh Hourly Performance (if needed)

# COMMAND ----------

def refresh_hourly_performance():
    """Refresh hourly performance aggregations"""
    log_info("Refreshing hourly_performance")
    
    try:
        # Check if we have query_performance_raw data
        raw_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {config['catalog']}.{config['schema']}.query_performance_raw").collect()[0]['cnt']
        
        if raw_count == 0:
            log_info("Skipping hourly_performance - no raw data available yet")
            return True
        
        # Get latest hour processed
        latest_hour = spark.sql(f"""
            SELECT COALESCE(
                MAX(TIMESTAMP(query_date, query_hour, 0, 0)),
                CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
            ) as latest
            FROM {config['catalog']}.{config['schema']}.hourly_performance
        """).collect()[0]['latest']
        
        # Process new hourly data
        hourly_data = spark.sql(f"""
            SELECT 
                DATE(start_time) as query_date,
                HOUR(start_time) as query_hour,
                workspace_id,
                user_id,
                COUNT(*) as query_count,
                COUNT(DISTINCT query_hash) as unique_query_patterns,
                AVG(duration_ms) as avg_duration_ms,
                percentile_approx(duration_ms, 0.5) as median_duration_ms,
                percentile_approx(duration_ms, 0.95) as p95_duration_ms,
                percentile_approx(duration_ms, 0.99) as p99_duration_ms,
                SUM(compute_cost_dbu) as total_cost_dbu,
                AVG(compute_cost_dbu) as avg_cost_dbu,
                SUM(bytes_read) as total_bytes_read,
                AVG(bytes_read) as avg_bytes_read,
                SUM(rows_read) as total_rows_read,
                AVG(rows_read) as avg_rows_read,
                SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) as successful_queries,
                SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) as failed_queries,
                AVG(CASE WHEN execution_status = 'FINISHED' THEN 1.0 ELSE 0.0 END) as success_rate,
                AVG(complexity_score) as avg_complexity_score,
                AVG(optimization_score) as avg_optimization_score,
                CURRENT_TIMESTAMP() as created_at
            FROM {config['catalog']}.{config['schema']}.query_performance_raw
            WHERE start_time > TIMESTAMP('{latest_hour}')
                AND DATE_TRUNC('HOUR', start_time) < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())
            GROUP BY DATE(start_time), HOUR(start_time), workspace_id, user_id
        """)
        
        if validate_data_quality(hourly_data, "hourly_performance", 0):
            if hourly_data.count() > 0:
                hourly_data.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(f"{config['catalog']}.{config['schema']}.hourly_performance")
                
                log_info(f"Successfully refreshed hourly_performance: {hourly_data.count()} hours processed")
            else:
                log_info("No new hourly data to process")
            
            return True
        else:
            return False
            
    except Exception as e:
        log_error(f"Failed to refresh hourly_performance: {str(e)}")
        return False

# Execute
success_3 = refresh_hourly_performance()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Results

# COMMAND ----------

# Summary
results = {
    "timestamp": datetime.now().isoformat(),
    "query_performance_categorized": success_1,
    "current_slow_queries": success_2, 
    "hourly_performance": success_3,
    "overall_success": all([success_1, success_2, success_3])
}

log_info(f"Refresh completed. Results: {json.dumps(results, indent=2)}")

# Display results for Databricks Jobs monitoring
if results["overall_success"]:
    print("✅ ALL TABLES REFRESHED SUCCESSFULLY")
    dbutils.notebook.exit("SUCCESS")
else:
    print("❌ SOME TABLES FAILED TO REFRESH")
    dbutils.notebook.exit("PARTIAL_FAILURE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling Instructions
# MAGIC 
# MAGIC 1. **Create Databricks Job**:
# MAGIC    - Go to Workflows → Jobs → Create Job
# MAGIC    - Task Type: Notebook
# MAGIC    - Notebook Path: This notebook
# MAGIC    - Schedule: Every 15 minutes or hourly
# MAGIC 
# MAGIC 2. **Parameters** (optional):
# MAGIC    - `incremental_hours`: How many hours back to process (default: 2)
# MAGIC    - `enable_logging`: Enable detailed logging (default: true)
# MAGIC 
# MAGIC 3. **Notifications**:
# MAGIC    - Email on failure
# MAGIC    - Slack integration if configured
# MAGIC 
# MAGIC 4. **Monitoring**:
# MAGIC    - Check job run history for success/failure
# MAGIC    - Monitor table row counts and data freshness
# MAGIC    - Set up alerts for job failures