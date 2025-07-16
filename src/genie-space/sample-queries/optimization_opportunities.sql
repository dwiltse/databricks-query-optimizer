-- Query Optimization Opportunities
-- Identify and prioritize optimization opportunities for maximum impact

-- Query 1: Top Optimization Opportunities by Impact
SELECT 
    qp.pattern_id,
    qp.pattern_type,
    qp.pattern_description,
    qp.optimization_recommendations,
    qp.occurrence_count,
    qp.avg_duration_ms,
    qp.avg_cost_dbu,
    qp.optimization_priority,
    mp.workspace_id,
    mp.execution_count,
    mp.total_cost_dbu,
    mp.estimated_monthly_savings_dbu,
    mp.optimization_impact,
    -- Calculate ROI metrics
    ROUND(mp.estimated_monthly_savings_dbu / NULLIF(mp.total_cost_dbu, 0) * 100, 2) as roi_percentage,
    ROUND(mp.estimated_monthly_savings_dbu * 30, 2) as potential_annual_savings_dbu,
    -- Implementation effort assessment
    CASE 
        WHEN qp.pattern_type IN ('SELECT_ALL', 'UNBOUNDED_SORT') THEN 'LOW'
        WHEN qp.pattern_type IN ('UNPARTITIONED_FILTER', 'REDUNDANT_DISTINCT') THEN 'MEDIUM'
        WHEN qp.pattern_type IN ('CARTESIAN_JOIN', 'HIGH_COMPLEXITY') THEN 'HIGH'
        ELSE 'MEDIUM'
    END as implementation_effort,
    -- Priority scoring (1-10, higher is better)
    CASE 
        WHEN mp.estimated_monthly_savings_dbu > 100 AND qp.pattern_type IN ('SELECT_ALL', 'UNBOUNDED_SORT') THEN 10
        WHEN mp.estimated_monthly_savings_dbu > 50 AND qp.pattern_type IN ('SELECT_ALL', 'UNBOUNDED_SORT') THEN 9
        WHEN mp.estimated_monthly_savings_dbu > 100 THEN 8
        WHEN mp.estimated_monthly_savings_dbu > 50 THEN 7
        WHEN mp.estimated_monthly_savings_dbu > 20 THEN 6
        WHEN mp.estimated_monthly_savings_dbu > 10 THEN 5
        WHEN mp.estimated_monthly_savings_dbu > 5 THEN 4
        WHEN mp.estimated_monthly_savings_dbu > 1 THEN 3
        ELSE 2
    END as priority_score
FROM mcp.query_optimization.query_patterns qp
JOIN mcp.query_optimization.mv_pattern_performance mp ON qp.query_hash = mp.query_hash
WHERE qp.last_seen >= current_date() - INTERVAL 7 DAYS
    AND mp.execution_count >= 5  -- Focus on patterns with significant usage
    AND mp.estimated_monthly_savings_dbu > 1  -- Meaningful savings threshold
ORDER BY priority_score DESC, mp.estimated_monthly_savings_dbu DESC
LIMIT 50;

-- Query 2: SELECT * Optimization Opportunities
SELECT 
    qpr.workspace_id,
    qpr.user_id,
    qpr.user_email,
    COUNT(*) as query_count,
    AVG(qpr.duration_ms) as avg_duration_ms,
    SUM(qpr.compute_cost_dbu) as total_cost_dbu,
    AVG(qpr.bytes_read) as avg_bytes_read,
    -- Estimate potential savings (typically 20-40% for SELECT * removal)
    SUM(qpr.compute_cost_dbu) * 0.3 as estimated_savings_dbu,
    SUM(qpr.compute_cost_dbu) * 0.3 * 30 as estimated_monthly_savings_dbu,
    -- Sample problematic queries
    COLLECT_LIST(LEFT(qpr.query_text, 100)) as sample_queries,
    -- Recommendation details
    'Replace SELECT * with specific column names to reduce data transfer and improve performance' as recommendation,
    'LOW' as implementation_effort,
    'HIGH' as business_impact,
    -- Calculate frequency
    ROUND(COUNT(*) / (SELECT COUNT(*) FROM mcp.query_optimization.query_performance_raw 
                      WHERE user_id = qpr.user_id AND start_time >= current_date() - INTERVAL 30 DAYS) * 100, 2) as usage_frequency_pct
FROM mcp.query_optimization.query_performance_raw qpr
WHERE qpr.start_time >= current_date() - INTERVAL 30 DAYS
    AND qpr.execution_status = 'FINISHED'
    AND UPPER(qpr.query_text) LIKE '%SELECT *%'
    AND qpr.bytes_read > 10485760  -- Only queries reading >10MB
GROUP BY qpr.workspace_id, qpr.user_id, qpr.user_email
HAVING COUNT(*) >= 5  -- Focus on users with multiple SELECT * queries
ORDER BY estimated_monthly_savings_dbu DESC
LIMIT 30;

-- Query 3: Unbounded Sort Optimization
SELECT 
    qpr.workspace_id,
    qpr.user_id,
    qpr.user_email,
    COUNT(*) as query_count,
    AVG(qpr.duration_ms) as avg_duration_ms,
    SUM(qpr.compute_cost_dbu) as total_cost_dbu,
    AVG(qpr.bytes_read) as avg_bytes_read,
    -- Estimate potential savings (typically 40-60% for adding LIMIT)
    SUM(qpr.compute_cost_dbu) * 0.5 as estimated_savings_dbu,
    SUM(qpr.compute_cost_dbu) * 0.5 * 30 as estimated_monthly_savings_dbu,
    -- Sample problematic queries
    COLLECT_LIST(LEFT(qpr.query_text, 150)) as sample_queries,
    -- Recommendation details
    'Add LIMIT clause to ORDER BY queries to prevent sorting entire datasets' as recommendation,
    'LOW' as implementation_effort,
    'HIGH' as business_impact,
    -- Average sort time
    AVG(qpr.duration_ms) as avg_sort_time_ms,
    -- Calculate the impact of sorting large datasets
    ROUND(AVG(qpr.bytes_read) / (1024 * 1024 * 1024.0), 2) as avg_gb_sorted
FROM mcp.query_optimization.query_performance_raw qpr
WHERE qpr.start_time >= current_date() - INTERVAL 30 DAYS
    AND qpr.execution_status = 'FINISHED'
    AND UPPER(qpr.query_text) LIKE '%ORDER BY%'
    AND UPPER(qpr.query_text) NOT LIKE '%LIMIT%'
    AND qpr.duration_ms > 30000  -- Only queries taking >30 seconds
GROUP BY qpr.workspace_id, qpr.user_id, qpr.user_email
HAVING COUNT(*) >= 3  -- Focus on users with multiple unbounded sorts
ORDER BY estimated_monthly_savings_dbu DESC
LIMIT 30;

-- Query 4: Large Data Scan Optimization
SELECT 
    qpr.workspace_id,
    qpr.user_id,
    qpr.user_email,
    COUNT(*) as query_count,
    AVG(qpr.duration_ms) as avg_duration_ms,
    SUM(qpr.compute_cost_dbu) as total_cost_dbu,
    AVG(qpr.bytes_read) as avg_bytes_read,
    ROUND(AVG(qpr.bytes_read) / (1024 * 1024 * 1024.0), 2) as avg_gb_read,
    MAX(qpr.bytes_read) as max_bytes_read,
    ROUND(MAX(qpr.bytes_read) / (1024 * 1024 * 1024.0), 2) as max_gb_read,
    -- Estimate potential savings (typically 30-50% with better partitioning/filtering)
    SUM(qpr.compute_cost_dbu) * 0.4 as estimated_savings_dbu,
    SUM(qpr.compute_cost_dbu) * 0.4 * 30 as estimated_monthly_savings_dbu,
    -- Analysis of scan patterns
    COUNT(CASE WHEN qpr.bytes_read > 5368709120 THEN 1 END) as large_scan_count,  -- >5GB
    COUNT(CASE WHEN qpr.bytes_read > 21474836480 THEN 1 END) as very_large_scan_count,  -- >20GB
    -- Sample queries
    COLLECT_LIST(LEFT(qpr.query_text, 150)) as sample_queries,
    -- Recommendations
    'Optimize data scanning by adding partition filters, improving table design, or using data skipping techniques' as recommendation,
    'MEDIUM' as implementation_effort,
    'HIGH' as business_impact,
    -- Calculate scan efficiency
    ROUND(SUM(qpr.bytes_read) / SUM(qpr.duration_ms) * 1000 / (1024 * 1024), 2) as mb_per_second_throughput
FROM mcp.query_optimization.query_performance_raw qpr
WHERE qpr.start_time >= current_date() - INTERVAL 30 DAYS
    AND qpr.execution_status = 'FINISHED'
    AND qpr.bytes_read > 1073741824  -- Only queries reading >1GB
    AND qpr.duration_ms > 60000  -- Only queries taking >1 minute
GROUP BY qpr.workspace_id, qpr.user_id, qpr.user_email
HAVING COUNT(*) >= 3  -- Focus on users with multiple large scans
ORDER BY estimated_monthly_savings_dbu DESC
LIMIT 30;

-- Query 5: High Cost Query Patterns
SELECT 
    qpr.query_hash,
    qp.pattern_type,
    qp.pattern_description,
    COUNT(*) as execution_count,
    COUNT(DISTINCT qpr.user_id) as unique_users,
    AVG(qpr.duration_ms) as avg_duration_ms,
    SUM(qpr.compute_cost_dbu) as total_cost_dbu,
    AVG(qpr.compute_cost_dbu) as avg_cost_dbu,
    MAX(qpr.compute_cost_dbu) as max_cost_dbu,
    -- Estimate monthly cost if pattern continues
    SUM(qpr.compute_cost_dbu) * 30 as estimated_monthly_cost_dbu,
    -- Potential savings with optimization
    SUM(qpr.compute_cost_dbu) * 0.35 * 30 as estimated_monthly_savings_dbu,
    -- Sample query text
    LEFT(ANY_VALUE(qpr.query_text), 200) as sample_query,
    -- Optimization recommendations
    qp.optimization_recommendations,
    -- Cost per execution trend
    ROUND(AVG(qpr.compute_cost_dbu) / AVG(qpr.duration_ms) * 1000, 4) as cost_per_second,
    -- Priority assessment
    CASE 
        WHEN SUM(qpr.compute_cost_dbu) > 100 THEN 'CRITICAL'
        WHEN SUM(qpr.compute_cost_dbu) > 50 THEN 'HIGH'
        WHEN SUM(qpr.compute_cost_dbu) > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END as optimization_priority
FROM mcp.query_optimization.query_performance_raw qpr
JOIN mcp.query_optimization.query_patterns qp ON qpr.query_hash = qp.query_hash
WHERE qpr.start_time >= current_date() - INTERVAL 30 DAYS
    AND qpr.execution_status = 'FINISHED'
    AND qpr.compute_cost_dbu > 5  -- Focus on queries with significant cost
GROUP BY qpr.query_hash, qp.pattern_type, qp.pattern_description, qp.optimization_recommendations
HAVING COUNT(*) >= 3  -- Focus on patterns with multiple executions
ORDER BY estimated_monthly_savings_dbu DESC
LIMIT 40;

-- Query 6: User-Specific Optimization Recommendations
SELECT 
    mu.user_id,
    mu.user_email,
    mu.workspace_id,
    mu.total_queries,
    mu.total_cost_dbu,
    mu.avg_optimization_score,
    mu.optimization_opportunity_score,
    -- Specific optimization areas
    COUNT(CASE WHEN qpr.query_text LIKE '%SELECT *%' THEN 1 END) as select_all_queries,
    COUNT(CASE WHEN qpr.query_text LIKE '%ORDER BY%' AND qpr.query_text NOT LIKE '%LIMIT%' THEN 1 END) as unbounded_sort_queries,
    COUNT(CASE WHEN qpr.bytes_read > 1073741824 THEN 1 END) as large_scan_queries,
    COUNT(CASE WHEN qpr.duration_ms > 300000 THEN 1 END) as long_running_queries,
    -- Potential savings by optimization type
    SUM(CASE WHEN qpr.query_text LIKE '%SELECT *%' THEN qpr.compute_cost_dbu * 0.3 ELSE 0 END) as select_all_savings,
    SUM(CASE WHEN qpr.query_text LIKE '%ORDER BY%' AND qpr.query_text NOT LIKE '%LIMIT%' THEN qpr.compute_cost_dbu * 0.5 ELSE 0 END) as unbounded_sort_savings,
    SUM(CASE WHEN qpr.bytes_read > 1073741824 THEN qpr.compute_cost_dbu * 0.4 ELSE 0 END) as large_scan_savings,
    SUM(CASE WHEN qpr.duration_ms > 300000 THEN qpr.compute_cost_dbu * 0.3 ELSE 0 END) as long_running_savings,
    -- Total potential savings
    (SUM(CASE WHEN qpr.query_text LIKE '%SELECT *%' THEN qpr.compute_cost_dbu * 0.3 ELSE 0 END) +
     SUM(CASE WHEN qpr.query_text LIKE '%ORDER BY%' AND qpr.query_text NOT LIKE '%LIMIT%' THEN qpr.compute_cost_dbu * 0.5 ELSE 0 END) +
     SUM(CASE WHEN qpr.bytes_read > 1073741824 THEN qpr.compute_cost_dbu * 0.4 ELSE 0 END) +
     SUM(CASE WHEN qpr.duration_ms > 300000 THEN qpr.compute_cost_dbu * 0.3 ELSE 0 END)) * 30 as total_monthly_savings_potential,
    -- Personalized recommendations
    CASE 
        WHEN COUNT(CASE WHEN qpr.query_text LIKE '%SELECT *%' THEN 1 END) > 5 THEN 'Focus on replacing SELECT * with specific columns'
        WHEN COUNT(CASE WHEN qpr.query_text LIKE '%ORDER BY%' AND qpr.query_text NOT LIKE '%LIMIT%' THEN 1 END) > 3 THEN 'Add LIMIT clauses to ORDER BY queries'
        WHEN COUNT(CASE WHEN qpr.bytes_read > 1073741824 THEN 1 END) > 3 THEN 'Optimize data scanning with better filtering'
        WHEN COUNT(CASE WHEN qpr.duration_ms > 300000 THEN 1 END) > 3 THEN 'Review long-running queries for optimization'
        ELSE 'General query optimization review recommended'
    END as primary_recommendation,
    -- Training/coaching priority
    CASE 
        WHEN mu.optimization_opportunity_score >= 8 THEN 'HIGH'
        WHEN mu.optimization_opportunity_score >= 6 THEN 'MEDIUM'
        ELSE 'LOW'
    END as coaching_priority
FROM mcp.query_optimization.mv_user_performance mu
JOIN mcp.query_optimization.query_performance_raw qpr ON mu.user_id = qpr.user_id 
    AND mu.workspace_id = qpr.workspace_id
WHERE mu.total_cost_dbu > 10  -- Focus on users with significant cost
    AND qpr.start_time >= current_date() - INTERVAL 30 DAYS
    AND qpr.execution_status = 'FINISHED'
GROUP BY mu.user_id, mu.user_email, mu.workspace_id, mu.total_queries, mu.total_cost_dbu, 
         mu.avg_optimization_score, mu.optimization_opportunity_score
HAVING (SUM(CASE WHEN qpr.query_text LIKE '%SELECT *%' THEN qpr.compute_cost_dbu * 0.3 ELSE 0 END) +
        SUM(CASE WHEN qpr.query_text LIKE '%ORDER BY%' AND qpr.query_text NOT LIKE '%LIMIT%' THEN qpr.compute_cost_dbu * 0.5 ELSE 0 END) +
        SUM(CASE WHEN qpr.bytes_read > 1073741824 THEN qpr.compute_cost_dbu * 0.4 ELSE 0 END) +
        SUM(CASE WHEN qpr.duration_ms > 300000 THEN qpr.compute_cost_dbu * 0.3 ELSE 0 END)) * 30 > 5
ORDER BY total_monthly_savings_potential DESC
LIMIT 50;

-- Query 7: Quick Wins - Low Effort, High Impact
SELECT 
    'Quick Win Opportunities' as category,
    qp.pattern_type,
    qp.pattern_description,
    COUNT(*) as occurrence_count,
    SUM(qpr.compute_cost_dbu) as total_cost_dbu,
    AVG(qpr.compute_cost_dbu) as avg_cost_dbu,
    -- Estimated savings (conservative estimates for quick wins)
    SUM(qpr.compute_cost_dbu) * 
    CASE 
        WHEN qp.pattern_type = 'SELECT_ALL' THEN 0.25
        WHEN qp.pattern_type = 'UNBOUNDED_SORT' THEN 0.40
        WHEN qp.pattern_type = 'REDUNDANT_DISTINCT' THEN 0.20
        WHEN qp.pattern_type = 'UNION_OPTIMIZATION' THEN 0.15
        ELSE 0.10
    END as estimated_savings_dbu,
    -- Implementation effort
    CASE 
        WHEN qp.pattern_type IN ('SELECT_ALL', 'REDUNDANT_DISTINCT', 'UNION_OPTIMIZATION') THEN 'LOW'
        WHEN qp.pattern_type = 'UNBOUNDED_SORT' THEN 'LOW'
        ELSE 'MEDIUM'
    END as implementation_effort,
    -- Expected implementation time
    CASE 
        WHEN qp.pattern_type IN ('SELECT_ALL', 'REDUNDANT_DISTINCT', 'UNION_OPTIMIZATION') THEN '< 1 hour per query'
        WHEN qp.pattern_type = 'UNBOUNDED_SORT' THEN '< 30 minutes per query'
        ELSE '1-2 hours per query'
    END as expected_implementation_time,
    -- Specific action items
    CASE 
        WHEN qp.pattern_type = 'SELECT_ALL' THEN 'Review tables and select only required columns'
        WHEN qp.pattern_type = 'UNBOUNDED_SORT' THEN 'Add appropriate LIMIT clauses'
        WHEN qp.pattern_type = 'REDUNDANT_DISTINCT' THEN 'Remove DISTINCT when using GROUP BY'
        WHEN qp.pattern_type = 'UNION_OPTIMIZATION' THEN 'Replace UNION with UNION ALL where appropriate'
        ELSE 'Standard optimization review'
    END as action_item,
    -- Sample queries for reference
    COLLECT_LIST(LEFT(qpr.query_text, 100)) as sample_queries
FROM mcp.query_optimization.query_patterns qp
JOIN mcp.query_optimization.query_performance_raw qpr ON qp.query_hash = qpr.query_hash
WHERE qpr.start_time >= current_date() - INTERVAL 30 DAYS
    AND qpr.execution_status = 'FINISHED'
    AND qp.pattern_type IN ('SELECT_ALL', 'UNBOUNDED_SORT', 'REDUNDANT_DISTINCT', 'UNION_OPTIMIZATION')
    AND qpr.compute_cost_dbu > 1  -- Focus on queries with measurable cost
GROUP BY qp.pattern_type, qp.pattern_description
HAVING COUNT(*) >= 5  -- Focus on patterns with multiple occurrences
ORDER BY estimated_savings_dbu DESC;

-- Query 8: Optimization Impact Tracking
SELECT 
    ot.optimization_type,
    ot.optimization_description,
    COUNT(*) as total_optimizations,
    AVG(ot.duration_improvement_pct) as avg_duration_improvement_pct,
    AVG(ot.cost_improvement_pct) as avg_cost_improvement_pct,
    SUM(ot.estimated_monthly_savings_usd) as total_estimated_monthly_savings,
    SUM(ot.actual_monthly_savings_usd) as total_actual_monthly_savings,
    -- Calculate accuracy of estimates
    ROUND(AVG(ot.actual_monthly_savings_usd / NULLIF(ot.estimated_monthly_savings_usd, 0)) * 100, 2) as estimate_accuracy_pct,
    -- Implementation success rate
    ROUND(COUNT(CASE WHEN ot.status = 'VERIFIED' THEN 1 END) * 100.0 / COUNT(*), 2) as implementation_success_rate,
    -- Average time to implement
    AVG(DATEDIFF(ot.updated_at, ot.created_at)) as avg_implementation_days,
    -- ROI calculation
    ROUND(SUM(ot.actual_monthly_savings_usd) / NULLIF(COUNT(*), 0), 2) as avg_savings_per_optimization
FROM mcp.query_optimization.optimization_tracking ot
WHERE ot.implementation_date >= current_date() - INTERVAL 90 DAYS
GROUP BY ot.optimization_type, ot.optimization_description
ORDER BY total_actual_monthly_savings DESC;