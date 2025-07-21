#!/usr/bin/env python3
"""
Enhanced MCP Server with PDF Knowledge Integration
Combines query performance data from Genie Spaces with optimization rules extracted from PDFs
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from mcp.server import Server
from mcp.types import Tool, TextContent
from databricks.sdk import WorkspaceClient
import difflib

@dataclass
class QueryAnalysis:
    """Query analysis result combining performance data and recommendations"""
    query_id: str
    performance_category: str
    optimization_flag: str
    execution_duration_ms: int
    bytes_per_row_efficiency: float
    matched_rules: List[Dict]
    recommended_optimizations: List[Dict]
    confidence_score: float

class KnowledgeBasedMCPServer:
    """MCP Server that combines Genie Space data with PDF-extracted optimization rules"""
    
    def __init__(self, knowledge_base_path: str = "knowledge_base/optimization_rules.json"):
        self.server = Server("databricks-query-optimizer-enhanced")
        self.workspace_client = WorkspaceClient()
        self.knowledge_base_path = Path(knowledge_base_path)
        self.optimization_rules = self._load_knowledge_base()
        self.setup_tools()
    
    def _load_knowledge_base(self) -> List[Dict]:
        """Load optimization rules from extracted PDF knowledge"""
        if not self.knowledge_base_path.exists():
            print(f"Warning: Knowledge base not found at {self.knowledge_base_path}")
            return []
        
        with open(self.knowledge_base_path, 'r') as f:
            rules = json.load(f)
        
        print(f"Loaded {len(rules)} optimization rules from knowledge base")
        return rules
    
    def setup_tools(self):
        """Setup MCP tools with knowledge-enhanced analysis"""
        
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="analyze_query_with_knowledge",
                    description="Analyze query performance using Genie Space data + PDF optimization knowledge",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query_id": {"type": "string", "description": "Query ID from Genie Space"},
                            "include_examples": {"type": "boolean", "default": True},
                            "optimization_focus": {
                                "type": "string", 
                                "enum": ["performance", "cost", "reliability", "all"],
                                "default": "all"
                            }
                        },
                        "required": ["query_id"]
                    }
                ),
                Tool(
                    name="get_optimization_recommendations",
                    description="Get optimization recommendations based on SQL query text and PDF knowledge",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "sql_query": {"type": "string", "description": "SQL query text to analyze"},
                            "performance_metrics": {
                                "type": "object",
                                "properties": {
                                    "execution_duration_ms": {"type": "integer"},
                                    "read_bytes": {"type": "integer"},
                                    "read_rows": {"type": "integer"}
                                }
                            },
                            "max_recommendations": {"type": "integer", "default": 5}
                        },
                        "required": ["sql_query"]
                    }
                ),
                Tool(
                    name="search_knowledge_base",
                    description="Search PDF-extracted optimization rules by query pattern or keywords",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "search_term": {"type": "string", "description": "Search term or SQL pattern"},
                            "optimization_type": {
                                "type": "string",
                                "enum": ["performance", "cost", "reliability", "general", "all"],
                                "default": "all"
                            },
                            "min_confidence": {"type": "number", "default": 0.5}
                        },
                        "required": ["search_term"]
                    }
                )
            ]
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            if name == "analyze_query_with_knowledge":
                return await self._analyze_query_with_knowledge(arguments)
            elif name == "get_optimization_recommendations":
                return await self._get_optimization_recommendations(arguments)
            elif name == "search_knowledge_base":
                return await self._search_knowledge_base(arguments)
            else:
                raise ValueError(f"Unknown tool: {name}")
    
    async def _analyze_query_with_knowledge(self, args: Dict[str, Any]) -> List[TextContent]:
        """Analyze specific query using both Genie Space data and PDF knowledge"""
        query_id = args["query_id"]
        include_examples = args.get("include_examples", True)
        optimization_focus = args.get("optimization_focus", "all")
        
        # Get query performance data from Genie Space
        query_data = await self._get_query_performance_data(query_id)
        if not query_data:
            return [TextContent(type="text", text=f"Query {query_id} not found in performance data")]
        
        # Match against PDF optimization rules
        matched_rules = self._match_optimization_rules(
            query_data['statement_text'],
            query_data,
            optimization_focus
        )
        
        # Generate comprehensive analysis
        analysis = self._generate_comprehensive_analysis(query_data, matched_rules, include_examples)
        
        return [TextContent(type="text", text=analysis)]
    
    async def _get_optimization_recommendations(self, args: Dict[str, Any]) -> List[TextContent]:
        """Get recommendations for SQL query using PDF knowledge base"""
        sql_query = args["sql_query"]
        performance_metrics = args.get("performance_metrics", {})
        max_recommendations = args.get("max_recommendations", 5)
        
        # Match query against optimization rules
        matched_rules = self._match_optimization_rules(sql_query, performance_metrics)
        
        # Rank and select top recommendations
        top_recommendations = sorted(matched_rules, key=lambda x: x['relevance_score'], reverse=True)[:max_recommendations]
        
        # Generate optimization report
        report = self._generate_optimization_report(sql_query, top_recommendations, performance_metrics)
        
        return [TextContent(type="text", text=report)]
    
    async def _search_knowledge_base(self, args: Dict[str, Any]) -> List[TextContent]:
        """Search optimization rules in knowledge base"""
        search_term = args["search_term"]
        optimization_type = args.get("optimization_type", "all")
        min_confidence = args.get("min_confidence", 0.5)
        
        # Filter rules by type and confidence
        filtered_rules = [
            rule for rule in self.optimization_rules
            if (optimization_type == "all" or rule["optimization_type"] == optimization_type)
            and rule["confidence_score"] >= min_confidence
        ]
        
        # Search by term
        matching_rules = []
        for rule in filtered_rules:
            search_text = f"{rule['title']} {rule['description']} {rule.get('query_pattern', '')}"
            if search_term.lower() in search_text.lower():
                matching_rules.append(rule)
        
        # Format results
        results = self._format_search_results(matching_rules, search_term)
        
        return [TextContent(type="text", text=results)]
    
    def _match_optimization_rules(self, sql_query: str, performance_data: Dict, focus_type: str = "all") -> List[Dict]:
        """Match SQL query against PDF optimization rules"""
        matched_rules = []
        
        for rule in self.optimization_rules:
            if focus_type != "all" and rule["optimization_type"] != focus_type:
                continue
            
            # Calculate relevance score
            relevance_score = self._calculate_rule_relevance(sql_query, rule, performance_data)
            
            if relevance_score > 0.3:  # Minimum relevance threshold
                rule_match = rule.copy()
                rule_match['relevance_score'] = relevance_score
                rule_match['applicability_reason'] = self._get_applicability_reason(sql_query, rule)
                matched_rules.append(rule_match)
        
        return matched_rules
    
    def _calculate_rule_relevance(self, sql_query: str, rule: Dict, performance_data: Dict) -> float:
        """Calculate how relevant an optimization rule is to the query"""
        score = 0.0
        
        # Pattern matching
        if rule.get('query_pattern'):
            pattern_similarity = difflib.SequenceMatcher(
                None, 
                sql_query.lower(), 
                rule['query_pattern'].lower()
            ).ratio()
            score += pattern_similarity * 0.4
        
        # Keyword matching
        rule_keywords = self._extract_keywords(rule['title'] + ' ' + rule['description'])
        query_keywords = self._extract_keywords(sql_query)
        
        keyword_overlap = len(rule_keywords.intersection(query_keywords)) / max(len(rule_keywords), 1)
        score += keyword_overlap * 0.3
        
        # Performance condition matching
        if rule.get('conditions'):
            condition_matches = self._check_conditions(performance_data, rule['conditions'])
            score += condition_matches * 0.2
        
        # Base confidence from PDF extraction
        score += rule.get('confidence_score', 0.5) * 0.1
        
        return min(1.0, score)
    
    def _extract_keywords(self, text: str) -> set:
        """Extract relevant keywords from text"""
        # Common SQL and performance keywords
        keywords = set()
        text_lower = text.lower()
        
        sql_keywords = ['select', 'where', 'join', 'group', 'order', 'distinct', 'union', 'limit', 'partition']
        perf_keywords = ['slow', 'fast', 'optimize', 'performance', 'cost', 'dbu', 'cache', 'index']
        
        for keyword in sql_keywords + perf_keywords:
            if keyword in text_lower:
                keywords.add(keyword)
        
        return keywords
    
    def _check_conditions(self, performance_data: Dict, conditions: List[str]) -> float:
        """Check how many conditions are met by the query performance data"""
        if not conditions:
            return 0.0
        
        met_conditions = 0
        for condition in conditions:
            if self._condition_applies(performance_data, condition):
                met_conditions += 1
        
        return met_conditions / len(conditions)
    
    def _condition_applies(self, performance_data: Dict, condition: str) -> bool:
        """Check if a condition applies to the performance data"""
        condition_lower = condition.lower()
        
        # Check for slow queries
        if 'slow' in condition_lower or 'long' in condition_lower:
            duration = performance_data.get('execution_duration_ms', 0)
            return duration > 300000  # > 5 minutes
        
        # Check for expensive queries
        if 'expensive' in condition_lower or 'cost' in condition_lower:
            dbu_cost = performance_data.get('dbu_consumed', 0)
            return dbu_cost > 5  # > 5 DBUs
        
        # Check for data scanning issues
        if 'scan' in condition_lower or 'read' in condition_lower:
            efficiency = performance_data.get('bytes_per_row_efficiency', 0)
            return efficiency > 10000  # > 10KB per row
        
        return False
    
    def _get_applicability_reason(self, sql_query: str, rule: Dict) -> str:
        """Get human-readable reason why rule applies"""
        reasons = []
        
        if rule.get('query_pattern'):
            if any(pattern in sql_query.lower() for pattern in ['select *', 'select distinct', 'order by']):
                reasons.append(f"Query matches pattern: {rule['query_pattern']}")
        
        if 'select *' in sql_query.lower() and 'select' in rule['title'].lower():
            reasons.append("Query uses SELECT * which can be optimized")
        
        if not reasons:
            reasons.append("General optimization principle applies")
        
        return "; ".join(reasons)
    
    def _generate_comprehensive_analysis(self, query_data: Dict, matched_rules: List[Dict], include_examples: bool) -> str:
        """Generate comprehensive analysis combining performance data and PDF knowledge"""
        analysis = f"""
# Query Performance Analysis

## Query Information
- **Query ID**: {query_data.get('statement_id', 'N/A')}
- **Performance Category**: {query_data.get('performance_category', 'N/A')}
- **Execution Duration**: {query_data.get('execution_duration_ms', 0)/1000:.1f} seconds
- **Optimization Flag**: {query_data.get('optimization_flag', 'N/A')}

## Query Text
```sql
{query_data.get('statement_text', 'N/A')}
```

## Performance Metrics
- **Data Processed**: {query_data.get('read_bytes', 0)/1024/1024:.1f} MB
- **Rows Processed**: {query_data.get('read_rows', 0):,}
- **Efficiency**: {query_data.get('bytes_per_row_efficiency', 0):.1f} bytes/row

## Optimization Recommendations
Found {len(matched_rules)} applicable optimization rules from knowledge base:

"""
        
        for i, rule in enumerate(matched_rules[:5], 1):
            analysis += f"""
### {i}. {rule['title']}
- **Type**: {rule['optimization_type'].title()}
- **Confidence**: {rule['confidence_score']:.2f}
- **Relevance**: {rule['relevance_score']:.2f}
- **Source**: {rule['source_document']} (page {rule['page_number']})

**Why this applies**: {rule.get('applicability_reason', 'General best practice')}

**Description**: {rule['description'][:200]}...

"""
            
            if include_examples and rule.get('before_example') and rule.get('after_example'):
                analysis += f"""
**Before**:
```sql
{rule['before_example']}
```

**After**:
```sql
{rule['after_example']}
```
"""
            
            if rule.get('expected_improvement'):
                analysis += f"**Expected Improvement**: {rule['expected_improvement']}\n"
            
            analysis += "\n---\n"
        
        return analysis
    
    def _generate_optimization_report(self, sql_query: str, recommendations: List[Dict], performance_metrics: Dict) -> str:
        """Generate optimization report based on recommendations"""
        report = f"""
# SQL Query Optimization Recommendations

## Query Analysis
```sql
{sql_query}
```

## Performance Context
"""
        if performance_metrics:
            report += f"""
- **Execution Time**: {performance_metrics.get('execution_duration_ms', 0)/1000:.1f} seconds
- **Data Read**: {performance_metrics.get('read_bytes', 0)/1024/1024:.1f} MB
- **Rows Processed**: {performance_metrics.get('read_rows', 0):,}
"""
        
        report += f"\n## Top {len(recommendations)} Recommendations\n"
        
        for i, rec in enumerate(recommendations, 1):
            report += f"""
### {i}. {rec['title']} (Relevance: {rec['relevance_score']:.2f})

{rec['description']}

**Optimization Type**: {rec['optimization_type'].title()}
"""
            
            if rec.get('expected_improvement'):
                report += f"**Expected Improvement**: {rec['expected_improvement']}\n"
            
            if rec.get('after_example'):
                report += f"""
**Optimized Version**:
```sql
{rec['after_example']}
```
"""
            
            report += f"*Source: {rec['source_document']} (page {rec['page_number']})*\n\n---\n"
        
        return report
    
    def _format_search_results(self, rules: List[Dict], search_term: str) -> str:
        """Format search results for display"""
        if not rules:
            return f"No optimization rules found for '{search_term}'"
        
        result = f"# Search Results for '{search_term}'\n\nFound {len(rules)} matching optimization rules:\n\n"
        
        for rule in rules[:10]:  # Limit to top 10
            result += f"""
## {rule['title']}
- **Type**: {rule['optimization_type'].title()}
- **Confidence**: {rule['confidence_score']:.2f}
- **Source**: {rule['source_document']} (page {rule['page_number']})

{rule['description'][:150]}...

---
"""
        
        return result
    
    async def _get_query_performance_data(self, query_id: str) -> Optional[Dict]:
        """Get query performance data from Genie Space (placeholder)"""
        # In real implementation, this would query your Genie Space
        # For now, return mock data structure
        return {
            'statement_id': query_id,
            'statement_text': 'SELECT * FROM large_table WHERE condition = value',
            'performance_category': 'SLOW',
            'optimization_flag': 'HEALTHY',
            'execution_duration_ms': 450000,
            'read_bytes': 10000000000,
            'read_rows': 1000000,
            'bytes_per_row_efficiency': 10000
        }

# Usage example
if __name__ == "__main__":
    server = KnowledgeBasedMCPServer("knowledge_base/optimization_rules.json")
    print("Enhanced MCP Server with PDF Knowledge Integration ready!")
    print(f"Loaded {len(server.optimization_rules)} optimization rules")