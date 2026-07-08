SELECT
  COUNT(*) AS row_count
  , SUM(`amount`) AS a0_sum
  , MIN(`amount`) AS a0_min
  , MAX(`amount`) AS a0_max
FROM `dq`.`customers`
WHERE (load_dt = '2026-01-01')
