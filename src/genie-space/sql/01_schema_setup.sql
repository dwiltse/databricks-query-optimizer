-- Query Performance Optimization Schema Setup
-- Complementary to existing mcp.cost_optimization schema
-- This schema focuses on query performance monitoring and optimization

-- Create the query optimization schema
CREATE SCHEMA IF NOT EXISTS mcp.query_optimization 
COMMENT 'Query performance optimization and monitoring schema - complements cost_optimization schema';

-- Use the schema for subsequent operations
USE mcp.query_optimization;

-- Enable Delta Lake features for optimal performance
SET spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension;
SET spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog;