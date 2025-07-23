#!/bin/bash
# 🚀 DEPLOY THE QUERY OPTIMIZATION BEAST!

set -e

APP_NAME="query-optimization-beast"
WORKSPACE_PATH="/Workspace/Users/$(databricks current-user me --output json | jq -r '.userName')/query_optimization_app"

echo "🚀 Deploying Query Optimization Beast..."
echo "App Name: $APP_NAME"
echo "Workspace Path: $WORKSPACE_PATH"

# Sync source code to workspace
echo "📁 Syncing code to workspace..."
databricks sync . "$WORKSPACE_PATH"

# Create the app (if it doesn't exist)
echo "🏗️ Creating Databricks App..."
databricks apps create "$APP_NAME" --source-code-path "$WORKSPACE_PATH" || echo "App might already exist, continuing..."

# Deploy the app
echo "🚀 Deploying app..."
databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH"

echo ""
echo "🎉 DEPLOYMENT COMPLETE!"
echo ""
echo "🔗 Your app should be available at:"
echo "   https://your-workspace.databricks.com/apps/$APP_NAME"
echo ""
echo "🛠️ To check status:"
echo "   databricks apps list"
echo "   databricks apps get $APP_NAME"
echo ""
echo "🔥 NOW GO OPTIMIZE SOME QUERIES!"