#!/bin/bash
# Claude Code Usage Monitor - Quick Commands

echo "🔍 Claude Code Usage Monitor"
echo "=============================="

# Quick current status
echo ""
echo "📊 Current Session Block:"
ccusage blocks | tail -5

echo ""
echo "💰 Today's Usage:"
ccusage daily | tail -5

echo ""
echo "🎯 Quick Commands:"
echo "  ccusage daily           - Daily breakdown"
echo "  ccusage session         - Session breakdown"  
echo "  ccusage blocks --live   - Live monitoring"
echo "  ccusage monthly         - Monthly totals"
echo ""
echo "💡 Pro Tip: Use 'ccusage blocks --live' for real-time monitoring"