SELECT
  COALESCE(CAST(CAST(`id` AS BIGINT) AS STRING), '<NULL>') AS k0,
  md5(CONCAT(COALESCE(RTRIM(`code`), '<NULL>'), '|~|', COALESCE(RTRIM(`name`), '<NULL>'), '|~|', COALESCE(CAST(CAST(`amount` AS DECIMAL(38,6)) AS STRING), '<NULL>'), '|~|', COALESCE(CAST(CAST(`ratio` AS DECIMAL(38,6)) AS STRING), '<NULL>'), '|~|', COALESCE(CAST(CAST(`units` AS BIGINT) AS STRING), '<NULL>'), '|~|', COALESCE(CASE WHEN `is_active` IS NULL THEN NULL WHEN `is_active` THEN '1' ELSE '0' END, '<NULL>'), '|~|', COALESCE(DATE_FORMAT(`born_on`, 'yyyy-MM-dd'), '<NULL>'), '|~|', COALESCE(DATE_FORMAT(`updated_at`, 'yyyy-MM-dd HH:mm:ss.SSSSSS'), '<NULL>'))) AS row_hash
FROM `dq`.`customers`
WHERE (load_dt = '2026-01-01')
