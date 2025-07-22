# Multi-Agent Workflow Plan for Databricks Query Optimization Project
**Date**: July 22, 2025  
**Purpose**: Structured approach to prevent errors and improve code quality using specialized AI agents

## **Core Agent Roles**

### **Agent 1: Builder/Developer**
**Primary Role**: Create code, scripts, and technical implementations
**Responsibilities**:
- Write SQL scripts, Python notebooks, configuration files
- Implement business logic and data transformations
- Create initial technical solutions
- Focus on functionality and meeting requirements

**When to Use**: Any creation task - new scripts, features, documentation

### **Agent 2: Documentation Validator** 
**Primary Role**: Verify all references against official documentation
**Responsibilities**:
- Cross-check field names against Azure Databricks docs
- Validate syntax against platform specifications
- Ensure compliance with Databricks best practices
- Catch schema mismatches and field name errors

**When to Use**: Before deploying any script that references system tables or external APIs

### **Agent 3: Schema/Architecture Reviewer**
**Primary Role**: Ensure consistency across files and proper relationships
**Responsibilities**:
- Verify table schemas match between create/populate scripts
- Check foreign key relationships and data flow
- Ensure consistent naming conventions across files
- Validate that dependent scripts align with table structures

**When to Use**: When creating scripts that interact with multiple tables or depend on existing schemas

### **Agent 4: Business Logic Validator**
**Primary Role**: Verify calculations and business rules are correct
**Responsibilities**:
- Check performance impact scoring algorithms
- Validate cost calculations and ROI formulas
- Ensure SLOW/MODERATE/FAST thresholds are consistently applied
- Review optimization recommendations for accuracy

**When to Use**: For any script involving calculations, scoring, or business rule implementations

### **Agent 5: Performance/Efficiency Optimizer**
**Primary Role**: Review for performance improvements and best practices
**Responsibilities**:
- Suggest query optimization techniques
- Identify inefficient SQL patterns
- Recommend indexing and partitioning strategies
- Check for unnecessary data scanning or complex operations

**When to Use**: After core functionality works, for optimization passes

## **Specialized Project Agents**

### **Agent 6: Genie Space Specialist**
**Primary Role**: Ensure compatibility with Databricks Genie Space requirements
**Responsibilities**:
- Verify ≤5 tables per Genie Space rule
- Check table relationships for natural language querying
- Ensure proper business context and field descriptions
- Validate example queries work as intended

### **Agent 7: MCP Integration Specialist**
**Primary Role**: Prepare for Model Context Protocol integration
**Responsibilities**:
- Structure data for AI consumption
- Ensure output formats support LLM analysis
- Design optimization recommendations for AI interpretation
- Plan context and prompt structures for MCP

### **Agent 8: Testing/Validation Specialist**
**Primary Role**: Create and execute comprehensive testing
**Responsibilities**:
- Design test cases for edge conditions
- Validate data quality and completeness
- Test scripts against different data scenarios
- Create regression testing approaches

## **Standard Workflow Patterns**

### **Pattern 1: New Script Creation**
```
Task: "Create new query analysis script"

Agent 1 (Builder): Creates initial script
→ Agent 2 (Doc Validator): Checks field names against Azure docs  
→ Agent 3 (Schema Reviewer): Verifies compatibility with existing tables
→ Agent 4 (Business Logic): Validates calculations and thresholds
→ Agent 5 (Optimizer): Suggests performance improvements
```

### **Pattern 2: Schema Changes**
```
Task: "Modify table structure"

Agent 1 (Builder): Creates schema changes
→ Agent 3 (Schema Reviewer): Checks impact on dependent scripts
→ Agent 2 (Doc Validator): Ensures compliance with platform standards
→ Agent 4 (Business Logic): Validates that business rules still work
```

### **Pattern 3: Bug Fixes**
```
Task: "Fix failing script"

Agent 2 (Doc Validator): Identifies field name/syntax issues
→ Agent 3 (Schema Reviewer): Checks for schema mismatches  
→ Agent 1 (Builder): Implements the fixes
→ Agent 4 (Business Logic): Verifies logic still works correctly
```

### **Pattern 4: Optimization Review**
```
Task: "Improve query performance"

Agent 5 (Optimizer): Analyzes current implementation for improvements
→ Agent 1 (Builder): Implements optimization suggestions
→ Agent 4 (Business Logic): Ensures optimizations don't break business rules
→ Agent 2 (Doc Validator): Confirms new approach follows best practices
```

## **Recommended Usage by Task Complexity**

### **Critical Tasks (Production Impact)**
**Agents**: Builder → Doc Validator → Schema Reviewer → Business Logic → Optimizer
**Use For**: New table creation, schema changes, production deployment scripts

### **Standard Development**
**Agents**: Builder → Doc Validator → Schema Reviewer
**Use For**: New analysis scripts, report queries, data transformations

### **Quick Fixes**
**Agents**: Doc Validator (identify issue) → Builder (implement fix)
**Use For**: Field name corrections, syntax fixes, minor adjustments

### **New Features**
**Agents**: Full pipeline depending on complexity
**Use For**: Major functionality additions, integration work, complex business logic

## **Multi-Agent Prompt Templates**

### **Template 1: Complex Script Development**
```
Create [SCRIPT_NAME] using multiple agents:

Agent 1: Build [specific functionality] with [requirements]
Agent 2: Validate all field references against [specific documentation]  
Agent 3: Ensure compatibility with [existing schemas/tables]
Agent 4: Verify [business logic/calculations] match [specific rules]
Agent 5: Optimize for [performance criteria]

#constraint each agent must approve before next agent proceeds
#constraint provide summary of findings from each agent
```

### **Template 2: Schema Modification**
```
Modify [TABLE_NAME] to [specific changes] using:

Agent 1: Design the schema change and migration approach
Agent 3: Analyze impact on [list dependent scripts]
Agent 2: Ensure compliance with [platform standards]
Agent 4: Verify business logic compatibility

#constraint provide complete impact analysis before implementation
#constraint include rollback plan
```

### **Template 3: Bug Investigation**
```
Debug [FAILING_SCRIPT] using investigative agents:

Agent 2: Check for documentation/syntax issues in [specific areas]
Agent 3: Verify schema compatibility with [related tables]
Agent 1: Implement fixes based on agent findings
Agent 4: Validate that fixes maintain [business requirements]

#constraint identify root cause before implementing fixes
```

### **Template 4: Quality Review**
```
Review [EXISTING_SCRIPT] for improvements using:

Agent 5: Analyze for performance optimization opportunities
Agent 2: Verify current implementation follows best practices
Agent 4: Check business logic accuracy and completeness
Agent 1: Implement approved improvements

#constraint prioritize changes by impact and effort
```

## **Best Practices for Multi-Agent Usage**

### **Agent Sequencing**
1. **Always start with Builder** for creation tasks
2. **Always include Doc Validator** for anything touching external systems
3. **Schema Reviewer before Business Logic** - structure before calculations
4. **Optimizer last** - functionality first, performance second

### **Quality Gates**
- Each agent must explicitly approve before proceeding to next agent
- Require summary reports from each agent
- Force agents to cite specific evidence (line numbers, documentation references)
- Include rollback/mitigation plans for complex changes

### **Communication Patterns**
```
Good: "Agent 1 create script, Agent 2 validate against Azure docs"
Better: "Agent 1 create script using business requirements X, Y, Z. Agent 2 validate all field names against Azure system.query.history documentation and report any mismatches with specific line references."
```

### **Error Prevention**
- Use constraints to force systematic validation
- Require agents to cross-reference their own work
- Build in checkpoints for complex workflows
- Document assumptions and decisions from each agent

## **Project-Specific Applications**

### **For Databricks Query Optimization:**
- **Always use Agent 2** for system table references
- **Always use Agent 6** for Genie Space compatibility
- **Use Agent 4** for any performance scoring or cost calculations
- **Consider Agent 7** when building MCP-ready outputs

### **For Production Deployment:**
- **Minimum**: Agents 1, 2, 3, 4 (Builder, Doc Validator, Schema, Business Logic)
- **Recommended**: Add Agent 8 (Testing) for comprehensive validation
- **Complex features**: Include Agent 5 (Performance) and relevant specialists

### **For POC/Demo Work:**
- **Minimum**: Agents 1, 2 (Builder, Doc Validator) 
- **Add Agent 6** for Genie Space demos
- **Add Agent 4** if calculations are involved

This multi-agent approach transforms error-prone single-agent development into a systematic quality assurance process, significantly reducing the debugging cycles and rework we experienced earlier in the project.