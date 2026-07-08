SELECT
  COALESCE(TO_VARCHAR(CAST("id" AS NUMBER(38,0))), '<NULL>') AS k0,
  MD5((COALESCE(RTRIM("code"), '<NULL>') || '|~|' || COALESCE(RTRIM("name"), '<NULL>') || '|~|' || COALESCE(TO_VARCHAR(CAST("amount" AS NUMBER(38,6))), '<NULL>') || '|~|' || COALESCE(TO_VARCHAR(CAST("ratio" AS NUMBER(38,6))), '<NULL>') || '|~|' || COALESCE(TO_VARCHAR(CAST("units" AS NUMBER(38,0))), '<NULL>') || '|~|' || COALESCE(CASE WHEN "is_active" IS NULL THEN NULL WHEN "is_active" THEN '1' ELSE '0' END, '<NULL>') || '|~|' || COALESCE(TO_VARCHAR("born_on", 'YYYY-MM-DD'), '<NULL>') || '|~|' || COALESCE(TO_VARCHAR("updated_at", 'YYYY-MM-DD HH24:MI:SS.FF6'), '<NULL>'))) AS row_hash
FROM "dq"."customers"
WHERE (load_dt = '2026-01-01')
