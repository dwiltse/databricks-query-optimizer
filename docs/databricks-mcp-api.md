# Databricks MCP API Documentation (Cached)
**Source**: https://docs.databricks.com/aws/en/generative-ai/mcp/
**Last Updated**: 2025-01-22

## Connection Pattern
```python
from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient(profile="your_profile")
mcp_client = DatabricksMCPClient(
    server_url=f"{workspace_client.config.host}/api/2.0/mcp/genie/{genie_space_id}", 
    workspace_client=workspace_client
)
```

## Query Method
```python
response = mcp_client.call_tool("query", {"question": user_question})
```

## Key Points
- **Beta Feature**: Requires serverless compute
- **URL Format**: `/api/2.0/mcp/genie/{genie_space_id}` (NOT `/functions/genie`)
- **Method**: `call_tool()` (NOT `query()`)
- **Parameters**: Tool name "query" with question in dict format

## Common Issues
- TaskGroup errors often indicate wrong URL format or missing Genie Space
- "No query attribute" means using wrong method name
- Connection succeeds but queries fail = wrong `call_tool` parameters