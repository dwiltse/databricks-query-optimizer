# API Documentation

## Overview
This document outlines the REST API endpoints for the Databricks Query Optimizer Platform.

## Base URL
```
https://api.databricks-query-optimizer.com/v1
```

## Authentication
All API requests require a valid JWT token in the Authorization header:
```
Authorization: Bearer <jwt_token>
```

## Core Endpoints

### 1. Query Performance API

#### Get Query Performance Details
```http
GET /queries/{query_id}/performance
```

**Parameters:**
- `query_id` (path, required): Unique identifier for the query

**Response:**
```json
{
  "query_id": "q_123456789",
  "workspace_id": "ws_001",
  "user_id": "user_123",
  "query_text": "SELECT * FROM sales WHERE date > '2023-01-01'",
  "performance_metrics": {
    "duration_ms": 45000,
    "bytes_read": 1048576,
    "rows_read": 10000,
    "compute_cost_dbu": 2.5,
    "execution_status": "FINISHED"
  },
  "resource_utilization": {
    "cpu_usage_percent": 75,
    "memory_usage_gb": 4.2,
    "disk_io_mb": 150
  },
  "optimization_score": 6.5,
  "recommendations": [
    {
      "type": "SELECT_OPTIMIZATION",
      "priority": "High",
      "description": "Replace SELECT * with specific columns",
      "estimated_savings_dbu": 0.8,
      "implementation_effort": "Low"
    }
  ]
}
```

#### Get Query Performance History
```http
GET /queries/{query_id}/history
```

**Parameters:**
- `query_id` (path, required): Unique identifier for the query
- `start_date` (query, optional): Start date for historical data
- `end_date` (query, optional): End date for historical data
- `limit` (query, optional): Maximum number of results (default: 100)

**Response:**
```json
{
  "query_id": "q_123456789",
  "history": [
    {
      "execution_date": "2023-12-01T10:00:00Z",
      "duration_ms": 45000,
      "compute_cost_dbu": 2.5,
      "execution_status": "FINISHED"
    }
  ],
  "performance_trend": {
    "avg_duration_ms": 42000,
    "trend_direction": "improving",
    "performance_change_percent": -5.2
  }
}
```

### 2. Dashboard Data API

#### Get Performance Overview
```http
GET /dashboard/performance-overview
```

**Parameters:**
- `workspace_id` (query, optional): Filter by workspace
- `start_date` (query, required): Start date for metrics
- `end_date` (query, required): End date for metrics
- `user_id` (query, optional): Filter by user

**Response:**
```json
{
  "summary": {
    "total_queries": 15847,
    "avg_duration_ms": 38000,
    "total_cost_dbu": 1250.75,
    "success_rate": 0.952,
    "performance_score": 7.2
  },
  "trends": [
    {
      "date": "2023-12-01",
      "query_count": 1205,
      "avg_duration_ms": 35000,
      "total_cost_dbu": 95.5,
      "success_rate": 0.956
    }
  ],
  "top_slow_queries": [
    {
      "query_id": "q_987654321",
      "duration_ms": 180000,
      "compute_cost_dbu": 15.2,
      "user_id": "user_456"
    }
  ]
}
```

#### Get Cost Analysis
```http
GET /dashboard/cost-analysis
```

**Parameters:**
- `workspace_id` (query, optional): Filter by workspace
- `start_date` (query, required): Start date for cost analysis
- `end_date` (query, required): End date for cost analysis
- `granularity` (query, optional): daily, weekly, monthly (default: daily)

**Response:**
```json
{
  "cost_summary": {
    "total_cost_dbu": 2485.50,
    "total_cost_usd": 497.10,
    "cost_per_query": 0.157,
    "projected_monthly_cost": 14913.00
  },
  "cost_breakdown": {
    "by_workspace": [
      {
        "workspace_id": "ws_001",
        "workspace_name": "Analytics",
        "cost_dbu": 1250.75,
        "cost_usd": 250.15,
        "query_count": 8500
      }
    ],
    "by_user": [
      {
        "user_id": "user_123",
        "user_name": "John Doe",
        "cost_dbu": 485.25,
        "cost_usd": 97.05,
        "query_count": 1250
      }
    ]
  },
  "cost_trends": [
    {
      "date": "2023-12-01",
      "total_cost_dbu": 95.5,
      "total_cost_usd": 19.10,
      "query_count": 1205
    }
  ]
}
```

### 3. Optimization API

#### Get Optimization Opportunities
```http
GET /optimization/opportunities
```

**Parameters:**
- `workspace_id` (query, optional): Filter by workspace
- `user_id` (query, optional): Filter by user
- `priority` (query, optional): High, Medium, Low
- `optimization_type` (query, optional): Filter by optimization type
- `limit` (query, optional): Maximum number of results (default: 50)

**Response:**
```json
{
  "opportunities": [
    {
      "opportunity_id": "opt_001",
      "query_id": "q_123456789",
      "workspace_id": "ws_001",
      "user_id": "user_123",
      "optimization_type": "SELECT_OPTIMIZATION",
      "priority": "High",
      "description": "Replace SELECT * with specific columns to reduce data transfer",
      "estimated_savings_dbu": 2.5,
      "estimated_savings_usd": 0.50,
      "implementation_effort": "Low",
      "created_at": "2023-12-01T10:00:00Z",
      "status": "pending"
    }
  ],
  "summary": {
    "total_opportunities": 156,
    "total_potential_savings_dbu": 487.5,
    "total_potential_savings_usd": 97.50,
    "high_priority_count": 25,
    "medium_priority_count": 89,
    "low_priority_count": 42
  }
}
```

#### Update Optimization Status
```http
PUT /optimization/opportunities/{opportunity_id}
```

**Parameters:**
- `opportunity_id` (path, required): Unique identifier for the optimization opportunity

**Request Body:**
```json
{
  "status": "implemented",
  "implementation_notes": "Updated query to select only required columns",
  "actual_savings_dbu": 2.3,
  "implementation_date": "2023-12-02T14:30:00Z"
}
```

**Response:**
```json
{
  "opportunity_id": "opt_001",
  "status": "implemented",
  "implementation_notes": "Updated query to select only required columns",
  "actual_savings_dbu": 2.3,
  "implementation_date": "2023-12-02T14:30:00Z",
  "updated_at": "2023-12-02T14:30:00Z"
}
```

### 4. User Analytics API

#### Get User Performance Summary
```http
GET /users/{user_id}/performance
```

**Parameters:**
- `user_id` (path, required): User identifier
- `start_date` (query, required): Start date for analysis
- `end_date` (query, required): End date for analysis

**Response:**
```json
{
  "user_id": "user_123",
  "user_name": "John Doe",
  "performance_summary": {
    "total_queries": 1250,
    "avg_duration_ms": 42000,
    "total_cost_dbu": 485.25,
    "success_rate": 0.948,
    "performance_score": 6.8
  },
  "query_patterns": [
    {
      "pattern_type": "SELECT_ALL",
      "query_count": 125,
      "avg_duration_ms": 65000,
      "total_cost_dbu": 95.5
    }
  ],
  "optimization_impact": {
    "recommendations_received": 25,
    "recommendations_implemented": 18,
    "total_savings_dbu": 45.2,
    "implementation_rate": 0.72
  }
}
```

### 5. Workspace Analytics API

#### Get Workspace Performance
```http
GET /workspaces/{workspace_id}/performance
```

**Parameters:**
- `workspace_id` (path, required): Workspace identifier
- `start_date` (query, required): Start date for analysis
- `end_date` (query, required): End date for analysis

**Response:**
```json
{
  "workspace_id": "ws_001",
  "workspace_name": "Analytics",
  "performance_summary": {
    "total_queries": 8500,
    "avg_duration_ms": 38000,
    "total_cost_dbu": 1250.75,
    "success_rate": 0.952,
    "active_users": 45
  },
  "top_users": [
    {
      "user_id": "user_123",
      "user_name": "John Doe",
      "query_count": 1250,
      "total_cost_dbu": 485.25
    }
  ],
  "resource_utilization": {
    "peak_concurrent_queries": 25,
    "avg_cluster_utilization": 0.68,
    "total_compute_hours": 125.5
  }
}
```

## Error Handling

### Error Response Format
```json
{
  "error": {
    "code": "INVALID_QUERY_ID",
    "message": "The specified query ID was not found",
    "details": "Query ID 'q_invalid' does not exist in the system",
    "timestamp": "2023-12-01T10:00:00Z"
  }
}
```

### HTTP Status Codes
- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Access denied
- `404 Not Found`: Resource not found
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server error

## Rate Limiting
- **Standard endpoints**: 100 requests per minute per user
- **Dashboard endpoints**: 1000 requests per minute per user
- **Bulk operations**: 10 requests per minute per user

## Pagination
For endpoints returning large datasets, pagination is supported:
```http
GET /queries?page=2&limit=50
```

**Response includes pagination metadata:**
```json
{
  "data": [...],
  "pagination": {
    "page": 2,
    "limit": 50,
    "total": 1500,
    "total_pages": 30,
    "has_next": true,
    "has_previous": true
  }
}
```

## TODO: API Implementation
- [ ] Implement authentication middleware
- [ ] Create API endpoint handlers
- [ ] Add request validation
- [ ] Implement rate limiting
- [ ] Add response caching
- [ ] Create API documentation
- [ ] Set up monitoring and logging
- [ ] Add automated testing
- [ ] Implement error handling
- [ ] Create SDK/client libraries