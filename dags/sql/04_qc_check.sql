-- 04_qc_check.sql
-- Lists the active data-quality checks that the qc_check task runs.
SELECT qc_id, src_query, tgt_query
FROM qc_checks
WHERE is_active
ORDER BY qc_id;
