SELECT
  COUNT(*) AS total_rows
  , SUM(CASE WHEN "name" IS NULL THEN 1 ELSE 0 END) AS c0
  , SUM(CASE WHEN "amount" IS NULL THEN 1 ELSE 0 END) AS c1
FROM "dq"."customers"
WHERE (load_dt = '2026-01-01')
