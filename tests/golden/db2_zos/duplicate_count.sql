SELECT COALESCE(SUM(dup_cnt - 1), 0) AS duplicate_rows
     , COUNT(*) AS duplicate_keys
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM "dq"."customers"
WHERE (load_dt = '2026-01-01')  GROUP BY "id"
  HAVING COUNT(*) > 1
) dvf_dup
