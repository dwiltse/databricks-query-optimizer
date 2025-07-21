#!/usr/bin/env python3
"""
Enhanced PDF Knowledge Extraction using Microsoft MarkItDown
Converts PDFs to Markdown first, then extracts structured optimization rules
"""

import re
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from markitdown import MarkItDown

@dataclass
class OptimizationRule:
    """Structured optimization rule extracted from markdown"""
    rule_id: str
    title: str
    description: str
    query_pattern: str
    optimization_type: str
    before_example: Optional[str] = None
    after_example: Optional[str] = None
    expected_improvement: Optional[str] = None
    conditions: List[str] = None
    source_document: str = ""
    confidence_score: float = 1.0

class MarkdownKnowledgeExtractor:
    """Extract optimization knowledge from PDFs via MarkItDown conversion"""
    
    def __init__(self, output_dir: str = "knowledge_base"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.markitdown = MarkItDown()
        
        # Enhanced patterns for markdown content
        self.patterns = {
            # Headers that indicate optimization content
            'optimization_headers': re.compile(
                r'^#+\s*(Optimization|Best Practice|Recommendation|Performance|Tip|Avoid|Use|Replace).*$',
                re.IGNORECASE | re.MULTILINE
            ),
            
            # Code blocks with SQL
            'sql_blocks': re.compile(
                r'```(?:sql|SQL)?\s*(.*?)\s*```',
                re.DOTALL | re.IGNORECASE
            ),
            
            # Before/After patterns in markdown
            'before_after': re.compile(
                r'(?:Before|❌|Don\'t|Instead of):\s*(.*?)\s*(?:After|✅|Do|Use|Better):\s*(.*?)(?=\n\n|\n#|\Z)',
                re.DOTALL | re.IGNORECASE
            ),
            
            # Performance improvements with numbers
            'performance_metrics': re.compile(
                r'(?:improve[sd]?|reduc[ed]|save[sd]?|faster|slower)\s+(?:by\s+)?[\w\s]*?(\d+(?:\.\d+)?[%xX]|\d+(?:\.\d+)?\s*times?)',
                re.IGNORECASE
            ),
            
            # Bullet points with conditions
            'bullet_conditions': re.compile(
                r'^[\s]*[-*+]\s*(.+?)$',
                re.MULTILINE
            ),
            
            # Tables with optimization comparisons
            'markdown_tables': re.compile(
                r'^\|.*?\|.*?\n\|.*?\|.*?\n((?:\|.*?\|.*?\n?)*)',
                re.MULTILINE
            )
        }
    
    def extract_from_pdf(self, pdf_path: str) -> List[OptimizationRule]:
        """Extract optimization rules from PDF by converting to markdown first"""
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            print(f"PDF file not found: {pdf_path}")
            return []
        
        print(f"Converting {pdf_path} to markdown...")
        
        # Convert PDF to markdown using MarkItDown
        try:
            result = self.markitdown.convert(str(pdf_file))
            markdown_content = result.text_content
            
            # Save intermediate markdown for debugging
            markdown_file = self.output_dir / f"{pdf_file.stem}_converted.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"Converted to markdown: {markdown_file}")
            
        except Exception as e:
            print(f"Error converting PDF to markdown: {e}")
            return []
        
        # Extract optimization rules from markdown
        rules = self._extract_rules_from_markdown(markdown_content, str(pdf_file))
        print(f"Extracted {len(rules)} rules from {pdf_path}")
        
        return rules
    
    def _extract_rules_from_markdown(self, markdown_content: str, source_doc: str) -> List[OptimizationRule]:
        """Extract optimization rules from markdown content"""
        rules = []
        
        # Split content by major headers
        sections = self._split_by_headers(markdown_content)
        
        for section in sections:
            rule = self._parse_optimization_section(section, source_doc)
            if rule:
                rules.append(rule)
        
        return rules
    
    def _split_by_headers(self, content: str) -> List[Dict[str, str]]:
        """Split markdown content into sections by headers"""
        sections = []
        
        # Find all optimization-related headers
        header_matches = list(self.patterns['optimization_headers'].finditer(content))
        
        for i, match in enumerate(header_matches):
            start_pos = match.start()
            end_pos = header_matches[i + 1].start() if i + 1 < len(header_matches) else len(content)
            
            section_content = content[start_pos:end_pos].strip()
            header_text = match.group(0).strip('#').strip()
            
            sections.append({
                'header': header_text,
                'content': section_content,
                'start_position': start_pos
            })
        
        return sections
    
    def _parse_optimization_section(self, section: Dict[str, str], source_doc: str) -> Optional[OptimizationRule]:
        """Parse a markdown section into an optimization rule"""
        content = section['content']
        header = section['header']
        
        if len(content) < 50:  # Skip very short sections
            return None
        
        # Extract SQL examples
        sql_blocks = self.patterns['sql_blocks'].findall(content)
        before_example = sql_blocks[0].strip() if sql_blocks else None
        after_example = sql_blocks[1].strip() if len(sql_blocks) > 1 else None
        
        # Look for before/after patterns in text
        before_after_match = self.patterns['before_after'].search(content)
        if before_after_match and not before_example:
            before_text = before_after_match.group(1).strip()
            after_text = before_after_match.group(2).strip()
            
            # Check if these contain SQL
            if any(keyword in before_text.lower() for keyword in ['select', 'from', 'where', 'join']):
                before_example = before_text
                after_example = after_text
        
        # Extract performance improvements
        perf_matches = self.patterns['performance_metrics'].findall(content)
        expected_improvement = perf_matches[0] if perf_matches else None
        
        # Extract conditions from bullet points
        conditions = []
        bullet_matches = self.patterns['bullet_conditions'].findall(content)
        for bullet in bullet_matches:
            bullet_clean = bullet.strip()
            if any(word in bullet_clean.lower() for word in ['when', 'if', 'for', 'use', 'avoid']):
                conditions.append(bullet_clean)
        
        # Generate description (first paragraph or first 300 chars)
        paragraphs = content.split('\n\n')
        description = ""
        for para in paragraphs[1:]:  # Skip header
            para_clean = para.strip()
            if para_clean and not para_clean.startswith('#') and not para_clean.startswith('```'):
                description = para_clean
                break
        
        if not description:
            description = content[:300] + "..." if len(content) > 300 else content
        
        # Determine optimization type
        opt_type = self._classify_optimization_type(header + " " + content)
        
        # Generate query pattern
        query_pattern = self._extract_query_pattern(before_example) if before_example else ""
        
        # Generate rule ID
        rule_id = hashlib.md5(f"{source_doc}_{header}".encode()).hexdigest()[:8]
        
        # Calculate confidence score
        confidence = self._calculate_confidence(content, before_example, after_example, expected_improvement)
        
        return OptimizationRule(
            rule_id=rule_id,
            title=header,
            description=description,
            query_pattern=query_pattern,
            optimization_type=opt_type,
            before_example=before_example,
            after_example=after_example,
            expected_improvement=expected_improvement,
            conditions=conditions if conditions else None,
            source_document=source_doc,
            confidence_score=confidence
        )
    
    def _classify_optimization_type(self, text: str) -> str:
        """Classify optimization type based on content"""
        text_lower = text.lower()
        
        # Performance indicators
        perf_keywords = ['performance', 'slow', 'fast', 'speed', 'duration', 'latency', 'throughput']
        if any(word in text_lower for word in perf_keywords):
            return 'performance'
        
        # Cost indicators
        cost_keywords = ['cost', 'dbu', 'billing', 'expensive', 'cheap', 'optimize cost', 'reduce spend']
        if any(word in text_lower for word in cost_keywords):
            return 'cost'
        
        # Reliability indicators
        reliability_keywords = ['error', 'failure', 'reliability', 'stable', 'crash', 'exception']
        if any(word in text_lower for word in reliability_keywords):
            return 'reliability'
        
        return 'general'
    
    def _extract_query_pattern(self, sql_example: Optional[str]) -> str:
        """Extract generalized query pattern from SQL example"""
        if not sql_example:
            return ""
        
        # Normalize SQL to pattern
        pattern = sql_example.strip()
        
        # Replace specific values with placeholders
        pattern = re.sub(r'\b\d+\b', 'N', pattern)  # Numbers
        pattern = re.sub(r"'[^']*'", "'...'", pattern)  # String literals
        pattern = re.sub(r'"[^"]*"', '"..."', pattern)  # Quoted identifiers
        pattern = re.sub(r'\b[a-zA-Z_]\w*\.[a-zA-Z_]\w*\.[a-zA-Z_]\w*', 'catalog.schema.table', pattern)  # 3-part names
        pattern = re.sub(r'\b[a-zA-Z_]\w*\.[a-zA-Z_]\w*', 'schema.table', pattern)  # 2-part names
        
        return pattern.strip()
    
    def _calculate_confidence(self, content: str, before: Optional[str], after: Optional[str], improvement: Optional[str]) -> float:
        """Calculate confidence score based on content quality"""
        score = 0.3  # Base score
        
        # Has clear before/after examples
        if before and after:
            score += 0.3
        
        # Has quantified improvement
        if improvement:
            score += 0.2
        
        # Has detailed explanation
        if len(content) > 200:
            score += 0.1
        
        # Has SQL code examples
        if '```sql' in content.lower() or '```SQL' in content:
            score += 0.1
        
        return min(1.0, score)
    
    def process_directory(self, pdf_directory: str) -> List[OptimizationRule]:
        """Process all PDFs in a directory"""
        pdf_dir = Path(pdf_directory)
        if not pdf_dir.exists():
            print(f"Directory not found: {pdf_directory}")
            return []
        
        all_rules = []
        pdf_files = list(pdf_dir.glob("*.pdf"))
        
        print(f"Found {len(pdf_files)} PDF files to process...")
        
        for pdf_file in pdf_files:
            print(f"\nProcessing: {pdf_file.name}")
            rules = self.extract_from_pdf(str(pdf_file))
            all_rules.extend(rules)
            print(f"  → Extracted {len(rules)} rules")
        
        return all_rules
    
    def save_knowledge_base(self, rules: List[OptimizationRule], format: str = 'json'):
        """Save extracted rules to knowledge base files"""
        if not rules:
            print("No rules to save")
            return
        
        if format == 'json':
            self._save_as_json(rules)
        elif format == 'markdown':
            self._save_as_markdown(rules)
        elif format == 'both':
            self._save_as_json(rules)
            self._save_as_markdown(rules)
    
    def _save_as_json(self, rules: List[OptimizationRule]):
        """Save as JSON for MCP server consumption"""
        rules_data = [asdict(rule) for rule in rules]
        
        output_file = self.output_dir / 'optimization_rules.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rules_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Saved {len(rules)} rules to {output_file}")
    
    def _save_as_markdown(self, rules: List[OptimizationRule]):
        """Save as structured markdown for review and documentation"""
        output_file = self.output_dir / 'optimization_rules_structured.md'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# SQL Query Optimization Knowledge Base\n\n")
            f.write(f"*Auto-extracted from PDF documentation using MarkItDown*\n\n")
            f.write(f"**Total Rules**: {len(rules)}\n\n")
            
            # Group by optimization type
            by_type = {}
            for rule in rules:
                if rule.optimization_type not in by_type:
                    by_type[rule.optimization_type] = []
                by_type[rule.optimization_type].append(rule)
            
            # Write table of contents
            f.write("## Table of Contents\n\n")
            for opt_type, type_rules in by_type.items():
                f.write(f"- [{opt_type.title()} Optimizations](#-{opt_type}-optimizations) ({len(type_rules)} rules)\n")
            f.write("\n")
            
            # Write sections
            for opt_type, type_rules in by_type.items():
                f.write(f"## 🎯 {opt_type.title()} Optimizations\n\n")
                
                for rule in sorted(type_rules, key=lambda x: x.confidence_score, reverse=True):
                    f.write(f"### {rule.title}\n\n")
                    
                    # Metadata
                    f.write("| Property | Value |\n")
                    f.write("|----------|-------|\n")
                    f.write(f"| Rule ID | `{rule.rule_id}` |\n")
                    f.write(f"| Source | {Path(rule.source_document).name} |\n")
                    f.write(f"| Confidence | {rule.confidence_score:.2f} |\n")
                    f.write(f"| Type | {rule.optimization_type.title()} |\n")
                    f.write("\n")
                    
                    # Description
                    f.write(f"**Description**: {rule.description}\n\n")
                    
                    # Query pattern
                    if rule.query_pattern:
                        f.write(f"**Query Pattern**:\n```sql\n{rule.query_pattern}\n```\n\n")
                    
                    # Conditions
                    if rule.conditions:
                        f.write("**Applies When**:\n")
                        for condition in rule.conditions:
                            f.write(f"- {condition}\n")
                        f.write("\n")
                    
                    # Examples
                    if rule.before_example:
                        f.write("**❌ Before (Problematic)**:\n```sql\n")
                        f.write(rule.before_example)
                        f.write("\n```\n\n")
                    
                    if rule.after_example:
                        f.write("**✅ After (Optimized)**:\n```sql\n")
                        f.write(rule.after_example)
                        f.write("\n```\n\n")
                    
                    # Expected improvement
                    if rule.expected_improvement:
                        f.write(f"**📈 Expected Improvement**: {rule.expected_improvement}\n\n")
                    
                    f.write("---\n\n")
        
        print(f"✅ Saved structured markdown to {output_file}")

# CLI usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract optimization rules from PDFs using MarkItDown")
    parser.add_argument("input", help="PDF file or directory containing PDFs")
    parser.add_argument("--output-dir", default="knowledge_base", help="Output directory for knowledge base")
    parser.add_argument("--format", choices=['json', 'markdown', 'both'], default='both', help="Output format")
    
    args = parser.parse_args()
    
    extractor = MarkdownKnowledgeExtractor(args.output_dir)
    
    input_path = Path(args.input)
    if input_path.is_file() and input_path.suffix == '.pdf':
        # Single PDF file
        rules = extractor.extract_from_pdf(str(input_path))
    elif input_path.is_dir():
        # Directory of PDFs
        rules = extractor.process_directory(str(input_path))
    else:
        print(f"Invalid input: {args.input}")
        exit(1)
    
    if rules:
        extractor.save_knowledge_base(rules, args.format)
        
        # Print summary
        by_type = {}
        for rule in rules:
            by_type[rule.optimization_type] = by_type.get(rule.optimization_type, 0) + 1
        
        print(f"\n📊 Summary:")
        print(f"Total rules extracted: {len(rules)}")
        for opt_type, count in sorted(by_type.items()):
            print(f"  {opt_type.title()}: {count} rules")
    else:
        print("No optimization rules found.")