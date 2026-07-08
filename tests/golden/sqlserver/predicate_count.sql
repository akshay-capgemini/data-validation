SELECT COUNT(*) AS violation_count
FROM [dq].[customers]
WHERE ([amount] < 0)
  AND ((load_dt = '2026-01-01'))
