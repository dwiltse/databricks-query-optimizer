#!/usr/bin/env python3
"""
PDF Knowledge Extraction for Query Optimization Recommendations
Converts PDF documents (Databricks optimization guides, best practices, etc.) 
into structured knowledge that MCP can use for recommendations.
"""

import pymupdf  # PyMuPDF
import re
import json
import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional
import hashlib

@dataclass
class OptimizationRule:
    """Structured optimization rule extracted from PDF"""
    rule_id: str
    title: str
    description: str
    query_pattern: str  # SQL pattern this applies to
    optimization_type: str  # 'performance', 'cost', 'reliability'
    before_example: Optional[str] = None
    after_example: Optional[str] = None
    expected_improvement: Optional[str] = None
    conditions: List[str] = None  # When this rule applies
    source_document: str = ""
    page_number: int = 0
    confidence_score: float = 1.0

class PDFKnowledgeExtractor:
    """Extract optimization knowledge from PDF documents"""
    
    def __init__(self, output_dir: str = "knowledge_base"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Patterns to identify optimization content
        self.patterns = {
            'optimization_header': re.compile(r'^(Optimization|Best Practice|Recommendation):\s*(.+)$', re.IGNORECASE | re.MULTILINE),
            'query_pattern': re.compile(r'```sql\s*(.*?)\s*```', re.DOTALL | re.IGNORECASE),
            'before_after': re.compile(r'(Before|Instead of):\s*(.*?)\s*(After|Use):\s*(.*?)(?=\n\n|\Z)', re.DOTALL | re.IGNORECASE),
            'performance_gain': re.compile(r'(?:improves?|reduces?|saves?)[^.]*?(\d+%|[\d.]+x)', re.IGNORECASE),
            'condition': re.compile(r'(?:when|if|for queries that)[^.]*?(?=\.|\n)', re.IGNORECASE)
        }
    
    def extract_from_pdf(self, pdf_path: str) -> List[OptimizationRule]:
        """Extract optimization rules from PDF document"""
        doc = pymupdf.open(pdf_path)
        rules = []
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text()
            
            # Extract optimization sections
            sections = self._identify_optimization_sections(text, page_num)
            
            for section in sections:
                rule = self._parse_optimization_section(section, pdf_path, page_num)
                if rule:
                    rules.append(rule)
        
        doc.close()
        return rules
    
    def _identify_optimization_sections(self, text: str, page_num: int) -> List[Dict]:
        """Identify sections containing optimization advice"""
        sections = []
        
        # Find optimization headers
        for match in self.patterns['optimization_header'].finditer(text):
            start_pos = match.start()
            title = match.group(2).strip()
            
            # Extract context around the optimization
            context_start = max(0, start_pos - 200)
            context_end = min(len(text), start_pos + 1000)
            context = text[context_start:context_end]
            
            sections.append({
                'title': title,
                'context': context,
                'page_number': page_num,
                'start_position': start_pos
            })
        
        return sections
    
    def _parse_optimization_section(self, section: Dict, source_doc: str, page_num: int) -> Optional[OptimizationRule]:
        """Parse optimization section into structured rule"""
        context = section['context']
        title = section['title']
        
        # Extract SQL examples
        sql_examples = self.patterns['query_pattern'].findall(context)
        before_example = sql_examples[0] if sql_examples else None
        after_example = sql_examples[1] if len(sql_examples) > 1 else None
        
        # Extract before/after patterns
        before_after_match = self.patterns['before_after'].search(context)
        if before_after_match and not before_example:
            before_example = before_after_match.group(2).strip()
            after_example = before_after_match.group(4).strip()
        
        # Extract performance improvements
        perf_match = self.patterns['performance_gain'].search(context)
        expected_improvement = perf_match.group(1) if perf_match else None
        
        # Extract conditions
        conditions = [match.group(0).strip() for match in self.patterns['condition'].finditer(context)]
        
        # Determine optimization type
        opt_type = self._classify_optimization_type(title + " " + context)
        
        # Generate rule ID
        rule_id = hashlib.md5(f"{source_doc}_{title}_{page_num}".encode()).hexdigest()[:8]
        
        return OptimizationRule(
            rule_id=rule_id,
            title=title,
            description=context[:500] + "..." if len(context) > 500 else context,
            query_pattern=self._extract_query_pattern(before_example),
            optimization_type=opt_type,
            before_example=before_example,
            after_example=after_example,
            expected_improvement=expected_improvement,
            conditions=conditions,
            source_document=source_doc,
            page_number=page_num,
            confidence_score=self._calculate_confidence(context, before_example, after_example)
        )
    
    def _classify_optimization_type(self, text: str) -> str:
        """Classify optimization type based on content"""
        text_lower = text.lower()
        if any(word in text_lower for word in ['cost', 'dbu', 'billing', 'expensive']):
            return 'cost'
        elif any(word in text_lower for word in ['performance', 'slow', 'duration', 'speed']):
            return 'performance'
        elif any(word in text_lower for word in ['error', 'failure', 'reliability', 'stable']):
            return 'reliability'
        else:
            return 'general'
    
    def _extract_query_pattern(self, sql_example: Optional[str]) -> str:
        """Extract generalized query pattern from SQL example"""
        if not sql_example:
            return ""
        
        # Normalize SQL to pattern
        pattern = re.sub(r'\b\d+\b', 'N', sql_example)  # Replace numbers
        pattern = re.sub(r"'[^']*'", "'...'", pattern)  # Replace string literals
        pattern = re.sub(r'\b[a-zA-Z_]\w*\.[a-zA-Z_]\w*', 'schema.table', pattern)  # Replace table references
        
        return pattern.strip()
    
    def _calculate_confidence(self, context: str, before: Optional[str], after: Optional[str]) -> float:
        """Calculate confidence score for the rule"""
        score = 0.5  # Base score
        
        if before and after:
            score += 0.3  # Has examples
        if self.patterns['performance_gain'].search(context):
            score += 0.2  # Has quantified improvement
        
        return min(1.0, score)
    
    def save_knowledge_base(self, rules: List[OptimizationRule], format: str = 'json'):
        """Save extracted rules to knowledge base files"""
        
        if format == 'json':
            self._save_as_json(rules)
        elif format == 'markdown':
            self._save_as_markdown(rules)
        elif format == 'yaml':
            self._save_as_yaml(rules)
    
    def _save_as_json(self, rules: List[OptimizationRule]):
        """Save as JSON for MCP server consumption"""
        rules_dict = [
            {
                'rule_id': rule.rule_id,
                'title': rule.title,
                'description': rule.description,
                'query_pattern': rule.query_pattern,
                'optimization_type': rule.optimization_type,
                'before_example': rule.before_example,
                'after_example': rule.after_example,
                'expected_improvement': rule.expected_improvement,
                'conditions': rule.conditions or [],
                'source_document': rule.source_document,
                'page_number': rule.page_number,
                'confidence_score': rule.confidence_score
            }
            for rule in rules
        ]
        
        output_file = self.output_dir / 'optimization_rules.json'
        with open(output_file, 'w') as f:
            json.dump(rules_dict, f, indent=2)
        
        print(f"Saved {len(rules)} rules to {output_file}")
    
    def _save_as_markdown(self, rules: List[OptimizationRule]):
        """Save as Markdown for documentation and review"""
        output_file = self.output_dir / 'optimization_rules.md'
        
        with open(output_file, 'w') as f:
            f.write("# Query Optimization Rules\n\n")
            f.write("*Extracted from PDF documentation*\n\n")
            
            # Group by optimization type
            by_type = {}
            for rule in rules:
                if rule.optimization_type not in by_type:
                    by_type[rule.optimization_type] = []
                by_type[rule.optimization_type].append(rule)
            
            for opt_type, type_rules in by_type.items():
                f.write(f"## {opt_type.title()} Optimizations\n\n")
                
                for rule in type_rules:
                    f.write(f"### {rule.title}\n\n")
                    f.write(f"**Rule ID**: `{rule.rule_id}`\n")
                    f.write(f"**Source**: {rule.source_document} (page {rule.page_number})\n")
                    f.write(f"**Confidence**: {rule.confidence_score:.2f}\n\n")
                    
                    f.write(f"{rule.description}\n\n")
                    
                    if rule.conditions:
                        f.write("**Applies when**:\n")
                        for condition in rule.conditions:
                            f.write(f"- {condition}\n")
                        f.write("\n")
                    
                    if rule.before_example:
                        f.write("**Before**:\n```sql\n")
                        f.write(rule.before_example)
                        f.write("\n```\n\n")
                    
                    if rule.after_example:
                        f.write("**After**:\n```sql\n")
                        f.write(rule.after_example)
                        f.write("\n```\n\n")
                    
                    if rule.expected_improvement:
                        f.write(f"**Expected Improvement**: {rule.expected_improvement}\n\n")
                    
                    f.write("---\n\n")
        
        print(f"Saved markdown documentation to {output_file}")

# Example usage
if __name__ == "__main__":
    extractor = PDFKnowledgeExtractor("../knowledge_base")
    
    # Extract from sample PDFs
    pdf_files = [
        "databricks_optimization_guide.pdf",
        "spark_sql_best_practices.pdf",
        "delta_lake_performance_tuning.pdf"
    ]
    
    all_rules = []
    for pdf_file in pdf_files:
        if Path(pdf_file).exists():
            rules = extractor.extract_from_pdf(pdf_file)
            all_rules.extend(rules)
            print(f"Extracted {len(rules)} rules from {pdf_file}")
    
    if all_rules:
        extractor.save_knowledge_base(all_rules, format='json')
        extractor.save_knowledge_base(all_rules, format='markdown')
        
        print(f"\nTotal: {len(all_rules)} optimization rules extracted")
        print(f"Performance rules: {len([r for r in all_rules if r.optimization_type == 'performance'])}")
        print(f"Cost rules: {len([r for r in all_rules if r.optimization_type == 'cost'])}")
        print(f"Reliability rules: {len([r for r in all_rules if r.optimization_type == 'reliability'])}")