SELECT
  COALESCE(VARCHAR(CAST("id" AS BIGINT)), '<NULL>') AS k0,
  LOWER(HEX(HASH_MD5((COALESCE(RTRIM("code"), '<NULL>') || '|~|' || COALESCE(RTRIM("name"), '<NULL>') || '|~|' || COALESCE(VARCHAR(CAST("amount" AS DECIMAL(31,6))), '<NULL>') || '|~|' || COALESCE(VARCHAR(CAST("ratio" AS DECIMAL(31,6))), '<NULL>') || '|~|' || COALESCE(VARCHAR(CAST("units" AS BIGINT)), '<NULL>') || '|~|' || COALESCE(CASE WHEN "is_active" IS NULL THEN NULL WHEN "is_active" = 0 THEN '0' ELSE '1' END, '<NULL>') || '|~|' || COALESCE(VARCHAR_FORMAT(TIMESTAMP("born_on"), 'YYYY-MM-DD'), '<NULL>') || '|~|' || COALESCE(VARCHAR_FORMAT("updated_at", 'YYYY-MM-DD HH24:MI:SS.FF6'), '<NULL>'))))) AS row_hash
FROM "dq"."customers"
WHERE (load_dt = '2026-01-01')
