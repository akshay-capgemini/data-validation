-- =====================================================================
--  SNOWFLAKE -> SNOWFLAKE DATA RECONCILIATION FRAMEWORK
-- ---------------------------------------------------------------------
--  Purpose : Compare SOURCE vs TARGET Snowflake tables and produce a
--            recon report (e.g. DataStage 11.7.1.1 output vs 11.7.1.6).
--
--  Checks  : schema diff (missing cols) ...... COLUMNS_ONLY_IN_ONE_SIDE
--            data-type diff ................... DATA_TYPE_MISMATCH
--            row count ........................ ROW_COUNT
--            full-data hash ................... FULL_DATA_HASH
--            duplicate keys ................... DUPLICATE_KEYS
--            key-level match .................. MATCHED / ONLY_IN_* / VALUE_DIFF
--            column-level hash (on failure) ... COLUMN_HASH: <col>
--            sample mismatched rows ........... RECON_MISMATCH_SAMPLE
--
--  HOW TO USE
--    1. Run PART 1 once (creates schema + tables + procedure).
--    2. Edit PART 2  -> add / change the table pairs you want to compare.
--    3. Run  PART 3  -> CALL UAT.RUN_RECON();
--    4. Run  PART 4  -> read the reports.
--
--  Safety: this script only CREATEs its own objects (IF NOT EXISTS) and does
--          INSERT / UPDATE (MERGE) on them. It never deletes or truncates any
--          table. Source and target tables are READ-ONLY (SELECT only).
--
--  Tip: for a single quick compare with no setup, use PART 0.
-- =====================================================================


-- =====================================================================
-- PART 0 : QUICK AD-HOC COMPARE (single table pair, no setup)
-- ---------------------------------------------------------------------
--  >>> EDIT THESE 4 VARIABLES, THEN RUN THE 3 QUERIES BELOW. <<<
--
--    SRC_TABLE / TGT_TABLE = fully qualified table names (DB.SCHEMA.TABLE)
--    FILTER    = optional WHERE clause, applied to BOTH tables.
--                Leave '' when not needed. Include the word WHERE.
--                Example (date column):  'WHERE LOAD_DT >= ''2026-01-01'''
--    ROW_LIMIT = max sample rows to return from the MINUS query.
-- =====================================================================
SET SRC_TABLE = 'SRC_DB.SRC_SCHEMA.MY_TABLE';
SET TGT_TABLE = 'TGT_DB.TGT_SCHEMA.MY_TABLE';
SET FILTER    = '';           -- e.g. 'WHERE LOAD_DT >= ''2026-01-01''' (leave '' if not required)
SET ROW_LIMIT = 200;

-- Row count
EXECUTE IMMEDIATE
       'SELECT (SELECT COUNT(*) FROM ' || $SRC_TABLE || ' ' || $FILTER || ') AS SRC_ROWS, '
    || '(SELECT COUNT(*) FROM ' || $TGT_TABLE || ' ' || $FILTER || ') AS TGT_ROWS';

-- Full data hash (order independent) - equal hash = identical data
EXECUTE IMMEDIATE
       'SELECT (SELECT HASH_AGG(*) FROM ' || $SRC_TABLE || ' ' || $FILTER || ') AS SRC_HASH, '
    || '(SELECT HASH_AGG(*) FROM ' || $TGT_TABLE || ' ' || $FILTER || ') AS TGT_HASH';

-- Rows present on one side but not the other (identical column order required)
EXECUTE IMMEDIATE
       'SELECT ''ONLY_IN_SOURCE'' AS SIDE, * FROM ('
    || '  SELECT * FROM ' || $SRC_TABLE || ' ' || $FILTER
    || '  MINUS SELECT * FROM ' || $TGT_TABLE || ' ' || $FILTER || ') '
    || 'UNION ALL '
    || 'SELECT ''ONLY_IN_TARGET'' AS SIDE, * FROM ('
    || '  SELECT * FROM ' || $TGT_TABLE || ' ' || $FILTER
    || '  MINUS SELECT * FROM ' || $SRC_TABLE || ' ' || $FILTER || ') '
    || 'LIMIT ' || $ROW_LIMIT;


-- =====================================================================
-- PART 1 : FRAMEWORK OBJECTS  (run once)
-- =====================================================================


-- One row per table pair to reconcile
CREATE TABLE IF NOT EXISTS UAT.RECON_CONFIG (
    RECON_ID      NUMBER AUTOINCREMENT,
    SRC_DB        VARCHAR,
    SRC_SCHEMA    VARCHAR,
    SRC_TABLE     VARCHAR,
    TGT_DB        VARCHAR,
    TGT_SCHEMA    VARCHAR,
    TGT_TABLE     VARCHAR,
    KEY_COLS      VARCHAR,          -- comma separated, e.g. 'CUST_ID,ORDER_ID' (NULL = no key, full-row compare)
    EXCLUDE_COLS  VARCHAR,          -- comma separated audit cols that always differ, e.g. 'LOAD_TS,BATCH_ID'
    ACTIVE_FLAG   CHAR(1) DEFAULT 'Y'
);

-- Check-level results for every run (the main recon report table)
CREATE TABLE IF NOT EXISTS UAT.DATASTAGE_MIGRATIONS_RECON_REPORT (
    RUN_ID        VARCHAR,
    RECON_ID      NUMBER,
    SRC_TABLE     VARCHAR,
    TGT_TABLE     VARCHAR,
    CHECK_NAME    VARCHAR,
    SRC_VALUE     VARCHAR,
    TGT_VALUE     VARCHAR,
    DIFF_VALUE    NUMBER,
    STATUS        VARCHAR,          -- PASS / FAIL / INFO / ERROR
    RUN_TS        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sample of mismatched rows (for analysis)
CREATE TABLE IF NOT EXISTS UAT.RECON_MISMATCH_SAMPLE (
    RUN_ID        VARCHAR,
    RECON_ID      NUMBER,
    SIDE          VARCHAR,          -- ONLY_IN_SOURCE / ONLY_IN_TARGET
    ROW_DATA      VARIANT,
    RUN_TS        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE PROCEDURE UAT.RUN_RECON(P_RECON_ID NUMBER DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id        VARCHAR DEFAULT UUID_STRING();
    v_id            NUMBER;
    v_ss            VARCHAR;  v_st VARCHAR;     -- source schema / table
    v_ts            VARCHAR;  v_tt VARCHAR;     -- target schema / table
    v_src           VARCHAR;  v_tgt VARCHAR;    -- quoted fully qualified names
    v_src_disp      VARCHAR;  v_tgt_disp VARCHAR;
    v_src_is        VARCHAR;  v_tgt_is VARCHAR; -- information_schema views
    v_cols          VARCHAR;                    -- common compared columns (quoted)
    v_hash_cols     VARCHAR;                    -- HASH_AGG per column
    v_src_only_cols VARCHAR;
    v_tgt_only_cols VARCHAR;
    v_type_diffs    VARCHAR;                    -- common cols with different data types
    v_keys          VARCHAR;
    v_key_list      VARCHAR;
    v_join          VARCHAR;
    v_excl          VARCHAR;
    v_hdr           VARCHAR;
    v_sql           VARCHAR;
    v_status        VARCHAR;
    v_err           VARCHAR;
    v_tables        NUMBER DEFAULT 0;
    e_no_cols EXCEPTION (-20001, 'No common columns found - check table names in UAT.RECON_CONFIG');
BEGIN
    LET cfg_rs RESULTSET := (
        SELECT * FROM UAT.RECON_CONFIG
         WHERE ACTIVE_FLAG = 'Y'
           AND (RECON_ID = :P_RECON_ID OR :P_RECON_ID IS NULL)
         ORDER BY RECON_ID);
    LET c_cfg CURSOR FOR cfg_rs;

    FOR rec IN c_cfg DO
        v_id       := rec.RECON_ID;
        v_ss       := UPPER(rec.SRC_SCHEMA);  v_st := UPPER(rec.SRC_TABLE);
        v_ts       := UPPER(rec.TGT_SCHEMA);  v_tt := UPPER(rec.TGT_TABLE);
        v_src      := '"' || UPPER(rec.SRC_DB) || '"."' || v_ss || '"."' || v_st || '"';
        v_tgt      := '"' || UPPER(rec.TGT_DB) || '"."' || v_ts || '"."' || v_tt || '"';
        v_src_disp := UPPER(rec.SRC_DB) || '.' || v_ss || '.' || v_st;
        v_tgt_disp := UPPER(rec.TGT_DB) || '.' || v_ts || '.' || v_tt;
        v_src_is   := UPPER(rec.SRC_DB) || '.INFORMATION_SCHEMA.COLUMNS';
        v_tgt_is   := UPPER(rec.TGT_DB) || '.INFORMATION_SCHEMA.COLUMNS';
        v_excl     := UPPER(REPLACE(COALESCE(rec.EXCLUDE_COLS, ''), ' ', ''));
        v_keys     := UPPER(REPLACE(COALESCE(rec.KEY_COLS, ''), ' ', ''));
        v_tables   := v_tables + 1;

        BEGIN
            -- ---------- Common columns (minus excluded) ----------
            SELECT LISTAGG('"' || s.COLUMN_NAME || '"', ', ')
                       WITHIN GROUP (ORDER BY s.ORDINAL_POSITION),
                   LISTAGG('HASH_AGG("' || s.COLUMN_NAME || '") AS "' || s.COLUMN_NAME || '"', ', ')
                       WITHIN GROUP (ORDER BY s.ORDINAL_POSITION)
              INTO :v_cols, :v_hash_cols
              FROM IDENTIFIER(:v_src_is) s
              JOIN IDENTIFIER(:v_tgt_is) t
                ON t.COLUMN_NAME = s.COLUMN_NAME
               AND t.TABLE_SCHEMA = :v_ts AND t.TABLE_NAME = :v_tt
             WHERE s.TABLE_SCHEMA = :v_ss AND s.TABLE_NAME = :v_st
               AND NOT ARRAY_CONTAINS(s.COLUMN_NAME::VARIANT, SPLIT(:v_excl, ','));

            IF (v_cols IS NULL OR v_cols = '') THEN
                RAISE e_no_cols;
            END IF;

            -- ---------- Schema check: columns present on only one side ----------
            SELECT COALESCE(LISTAGG(s.COLUMN_NAME, ', '), '') INTO :v_src_only_cols
              FROM IDENTIFIER(:v_src_is) s
              LEFT JOIN IDENTIFIER(:v_tgt_is) t
                ON t.COLUMN_NAME = s.COLUMN_NAME
               AND t.TABLE_SCHEMA = :v_ts AND t.TABLE_NAME = :v_tt
             WHERE s.TABLE_SCHEMA = :v_ss AND s.TABLE_NAME = :v_st
               AND t.COLUMN_NAME IS NULL;

            SELECT COALESCE(LISTAGG(t.COLUMN_NAME, ', '), '') INTO :v_tgt_only_cols
              FROM IDENTIFIER(:v_tgt_is) t
              LEFT JOIN IDENTIFIER(:v_src_is) s
                ON s.COLUMN_NAME = t.COLUMN_NAME
               AND s.TABLE_SCHEMA = :v_ss AND s.TABLE_NAME = :v_st
             WHERE t.TABLE_SCHEMA = :v_ts AND t.TABLE_NAME = :v_tt
               AND s.COLUMN_NAME IS NULL;

            INSERT INTO UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
                   (RUN_ID, RECON_ID, SRC_TABLE, TGT_TABLE, CHECK_NAME, SRC_VALUE, TGT_VALUE, DIFF_VALUE, STATUS)
            SELECT :v_run_id, :v_id, :v_src_disp, :v_tgt_disp, 'COLUMNS_ONLY_IN_ONE_SIDE',
                   NULLIF(:v_src_only_cols, ''), NULLIF(:v_tgt_only_cols, ''), NULL,
                   IFF(:v_src_only_cols = '' AND :v_tgt_only_cols = '', 'PASS', 'FAIL');

            -- ---------- Schema check: common columns with different data types ----------
            SELECT COALESCE(LISTAGG(s.COLUMN_NAME || ' (' || s.DATA_TYPE || ' vs ' || t.DATA_TYPE || ')', ', ')
                                WITHIN GROUP (ORDER BY s.ORDINAL_POSITION), '')
              INTO :v_type_diffs
              FROM IDENTIFIER(:v_src_is) s
              JOIN IDENTIFIER(:v_tgt_is) t
                ON t.COLUMN_NAME = s.COLUMN_NAME
               AND t.TABLE_SCHEMA = :v_ts AND t.TABLE_NAME = :v_tt
             WHERE s.TABLE_SCHEMA = :v_ss AND s.TABLE_NAME = :v_st
               AND s.DATA_TYPE <> t.DATA_TYPE
               AND NOT ARRAY_CONTAINS(s.COLUMN_NAME::VARIANT, SPLIT(:v_excl, ','));

            INSERT INTO UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
                   (RUN_ID, RECON_ID, SRC_TABLE, TGT_TABLE, CHECK_NAME, SRC_VALUE, TGT_VALUE, DIFF_VALUE, STATUS)
            SELECT :v_run_id, :v_id, :v_src_disp, :v_tgt_disp, 'DATA_TYPE_MISMATCH',
                   NULLIF(:v_type_diffs, ''), NULL, NULL,
                   IFF(:v_type_diffs = '', 'PASS', 'FAIL');

            -- Common prefix for all dynamic result inserts
            v_hdr := 'INSERT INTO UAT.DATASTAGE_MIGRATIONS_RECON_REPORT (RUN_ID, RECON_ID, SRC_TABLE, TGT_TABLE, CHECK_NAME, '
                  || 'SRC_VALUE, TGT_VALUE, DIFF_VALUE, STATUS) SELECT '''
                  || v_run_id || ''', ' || v_id || ', ''' || v_src_disp || ''', ''' || v_tgt_disp || ''', ';

            -- ---------- Row count ----------
            v_sql := v_hdr || '''ROW_COUNT'', s.C::VARCHAR, t.C::VARCHAR, s.C - t.C, '
                  || 'IFF(s.C = t.C, ''PASS'', ''FAIL'') '
                  || 'FROM (SELECT COUNT(*) C FROM ' || v_src || ') s, '
                  || '(SELECT COUNT(*) C FROM ' || v_tgt || ') t';
            EXECUTE IMMEDIATE :v_sql;

            -- ---------- Full data hash ----------
            v_sql := v_hdr || '''FULL_DATA_HASH'', s.H::VARCHAR, t.H::VARCHAR, NULL, '
                  || 'IFF(EQUAL_NULL(s.H, t.H), ''PASS'', ''FAIL'') '
                  || 'FROM (SELECT HASH_AGG(' || v_cols || ') H FROM ' || v_src || ') s, '
                  || '(SELECT HASH_AGG(' || v_cols || ') H FROM ' || v_tgt || ') t';
            EXECUTE IMMEDIATE :v_sql;

            IF (v_keys <> '') THEN
                SELECT LISTAGG('s."' || VALUE || '" = t."' || VALUE || '"', ' AND '),
                       LISTAGG('"' || VALUE || '"', ', ')
                  INTO :v_join, :v_key_list
                  FROM TABLE(SPLIT_TO_TABLE(:v_keys, ','));

                -- ---------- Duplicate keys ----------
                v_sql := v_hdr || '''DUPLICATE_KEYS'', s.C::VARCHAR, t.C::VARCHAR, s.C - t.C, '
                      || 'IFF(s.C = 0 AND t.C = 0, ''PASS'', ''FAIL'') '
                      || 'FROM (SELECT COUNT(*) C FROM (SELECT ' || v_key_list || ' FROM ' || v_src
                      || ' GROUP BY ' || v_key_list || ' HAVING COUNT(*) > 1)) s, '
                      || '(SELECT COUNT(*) C FROM (SELECT ' || v_key_list || ' FROM ' || v_tgt
                      || ' GROUP BY ' || v_key_list || ' HAVING COUNT(*) > 1)) t';
                EXECUTE IMMEDIATE :v_sql;

                -- ---------- Key-level comparison ----------
                v_sql := v_hdr || 'CHECK_NAME, NULL, NULL, CNT, '
                      || 'IFF(CHECK_NAME = ''MATCHED_KEYS'', ''INFO'', IFF(CNT = 0, ''PASS'', ''FAIL'')) '
                      || 'FROM (SELECT '
                      || 'COUNT_IF(s.S_ = 1 AND t.T_ = 1) AS MATCHED_KEYS, '
                      || 'COUNT_IF(t.T_ IS NULL) AS KEYS_ONLY_IN_SOURCE, '
                      || 'COUNT_IF(s.S_ IS NULL) AS KEYS_ONLY_IN_TARGET, '
                      || 'COUNT_IF(s.S_ = 1 AND t.T_ = 1 AND s.H_ <> t.H_) AS KEYS_WITH_VALUE_DIFF '
                      || 'FROM (SELECT ' || v_key_list || ', HASH(' || v_cols || ') H_, 1 S_ FROM ' || v_src || ') s '
                      || 'FULL OUTER JOIN (SELECT ' || v_key_list || ', HASH(' || v_cols || ') H_, 1 T_ FROM ' || v_tgt || ') t '
                      || 'ON ' || v_join || ') '
                      || 'UNPIVOT (CNT FOR CHECK_NAME IN (MATCHED_KEYS, KEYS_ONLY_IN_SOURCE, '
                      || 'KEYS_ONLY_IN_TARGET, KEYS_WITH_VALUE_DIFF))';
                EXECUTE IMMEDIATE :v_sql;
            ELSE
                -- ---------- No key: full-row MINUS both directions ----------
                v_sql := v_hdr || 'CHECK_NAME, NULL, NULL, CNT, IFF(CNT = 0, ''PASS'', ''FAIL'') '
                      || 'FROM (SELECT '
                      || '(SELECT COUNT(*) FROM (SELECT ' || v_cols || ' FROM ' || v_src
                      || ' MINUS SELECT ' || v_cols || ' FROM ' || v_tgt || ')) AS ROWS_ONLY_IN_SOURCE, '
                      || '(SELECT COUNT(*) FROM (SELECT ' || v_cols || ' FROM ' || v_tgt
                      || ' MINUS SELECT ' || v_cols || ' FROM ' || v_src || ')) AS ROWS_ONLY_IN_TARGET) '
                      || 'UNPIVOT (CNT FOR CHECK_NAME IN (ROWS_ONLY_IN_SOURCE, ROWS_ONLY_IN_TARGET))';
                EXECUTE IMMEDIATE :v_sql;
            END IF;

            -- ---------- Drill-down only when data hash failed ----------
            SELECT MAX(STATUS) INTO :v_status
              FROM UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
             WHERE RUN_ID = :v_run_id AND RECON_ID = :v_id AND CHECK_NAME = 'FULL_DATA_HASH';

            IF (v_status = 'FAIL') THEN
                -- Column-level hash: shows exactly which columns differ
                v_sql := v_hdr || '''COLUMN_HASH: '' || s.COL, s.H::VARCHAR, t.H::VARCHAR, NULL, '
                      || 'IFF(EQUAL_NULL(s.H, t.H), ''PASS'', ''FAIL'') '
                      || 'FROM (SELECT * FROM (SELECT ' || v_hash_cols || ' FROM ' || v_src || ') '
                      || 'UNPIVOT (H FOR COL IN (' || v_cols || '))) s '
                      || 'JOIN (SELECT * FROM (SELECT ' || v_hash_cols || ' FROM ' || v_tgt || ') '
                      || 'UNPIVOT (H FOR COL IN (' || v_cols || '))) t ON s.COL = t.COL';
                EXECUTE IMMEDIATE :v_sql;

                -- Sample mismatched rows (100 per side)
                v_sql := 'INSERT INTO UAT.RECON_MISMATCH_SAMPLE (RUN_ID, RECON_ID, SIDE, ROW_DATA) '
                      || 'SELECT ''' || v_run_id || ''', ' || v_id || ', ''ONLY_IN_SOURCE'', OBJECT_CONSTRUCT_KEEP_NULL(*) '
                      || 'FROM (SELECT ' || v_cols || ' FROM ' || v_src
                      || ' MINUS SELECT ' || v_cols || ' FROM ' || v_tgt || ') LIMIT 100';
                EXECUTE IMMEDIATE :v_sql;

                v_sql := 'INSERT INTO UAT.RECON_MISMATCH_SAMPLE (RUN_ID, RECON_ID, SIDE, ROW_DATA) '
                      || 'SELECT ''' || v_run_id || ''', ' || v_id || ', ''ONLY_IN_TARGET'', OBJECT_CONSTRUCT_KEEP_NULL(*) '
                      || 'FROM (SELECT ' || v_cols || ' FROM ' || v_tgt
                      || ' MINUS SELECT ' || v_cols || ' FROM ' || v_src || ') LIMIT 100';
                EXECUTE IMMEDIATE :v_sql;
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_err := SQLERRM;
                INSERT INTO UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
                       (RUN_ID, RECON_ID, SRC_TABLE, TGT_TABLE, CHECK_NAME, SRC_VALUE, TGT_VALUE, DIFF_VALUE, STATUS)
                SELECT :v_run_id, :v_id, :v_src_disp, :v_tgt_disp, 'ERROR', :v_err, NULL, NULL, 'ERROR';
        END;
    END FOR;

    RETURN 'RUN_ID = ' || v_run_id || ' | tables reconciled = ' || v_tables;
END;
$$;


-- =====================================================================
-- PART 2 : CONFIGURE TABLE PAIRS
--          >>> THIS IS THE ONLY SECTION YOU NORMALLY EDIT. <<<
--
--  Columns:
--    SRC_DB, SRC_SCHEMA, SRC_TABLE  = the SOURCE (old) table
--    TGT_DB, TGT_SCHEMA, TGT_TABLE  = the TARGET (new) table
--    KEY_COLS      = primary/business key, comma separated. NULL = full-row compare.
--    EXCLUDE_COLS  = audit cols to ignore (load timestamps, batch ids). NULL = none.
--
--  To add a table : copy a line, change the values.
--  To skip a table: set ACTIVE_FLAG = 'N' (see UPDATE example at bottom of PART 2).
-- =====================================================================

-- Upsert the table pairs (INSERT new / UPDATE existing). No TRUNCATE or DELETE is
-- used, so re-running is always safe and never removes rows. Matched on the 6
-- source/target name columns. All names below are generic placeholders - replace them.
MERGE INTO UAT.RECON_CONFIG AS cfg
USING (
    SELECT * FROM VALUES
    --   SRC_DB    SRC_SCHEMA    SRC_TABLE       TGT_DB    TGT_SCHEMA    TGT_TABLE       KEY_COLS               EXCLUDE_COLS
        ('SRC_DB', 'SRC_SCHEMA', 'TABLE_NAME_1', 'TGT_DB', 'TGT_SCHEMA', 'TABLE_NAME_1', 'KEY_COL',             'AUDIT_COL_1,AUDIT_COL_2'),
        ('SRC_DB', 'SRC_SCHEMA', 'TABLE_NAME_2', 'TGT_DB', 'TGT_SCHEMA', 'TABLE_NAME_2', 'KEY_COL_1,KEY_COL_2', 'AUDIT_COL'),
        ('SRC_DB', 'SRC_SCHEMA', 'TABLE_NAME_3', 'TGT_DB', 'TGT_SCHEMA', 'TABLE_NAME_3', NULL,                  NULL)
    --  , ('SRC_DB','SRC_SCHEMA','TABLE_NAME_N','TGT_DB','TGT_SCHEMA','TABLE_NAME_N','KEY_COL', NULL)   -- copy a line to add more
      AS v(SRC_DB, SRC_SCHEMA, SRC_TABLE, TGT_DB, TGT_SCHEMA, TGT_TABLE, KEY_COLS, EXCLUDE_COLS)
) AS src
   ON  cfg.SRC_DB = src.SRC_DB AND cfg.SRC_SCHEMA = src.SRC_SCHEMA AND cfg.SRC_TABLE = src.SRC_TABLE
   AND cfg.TGT_DB = src.TGT_DB AND cfg.TGT_SCHEMA = src.TGT_SCHEMA AND cfg.TGT_TABLE = src.TGT_TABLE
WHEN MATCHED THEN UPDATE
   SET cfg.KEY_COLS = src.KEY_COLS, cfg.EXCLUDE_COLS = src.EXCLUDE_COLS, cfg.ACTIVE_FLAG = 'Y'
WHEN NOT MATCHED THEN INSERT
   (SRC_DB, SRC_SCHEMA, SRC_TABLE, TGT_DB, TGT_SCHEMA, TGT_TABLE, KEY_COLS, EXCLUDE_COLS, ACTIVE_FLAG)
   VALUES (src.SRC_DB, src.SRC_SCHEMA, src.SRC_TABLE, src.TGT_DB, src.TGT_SCHEMA, src.TGT_TABLE,
           src.KEY_COLS, src.EXCLUDE_COLS, 'Y');

-- Example: temporarily skip a table without deleting it (UPDATE only, no DELETE)
-- UPDATE UAT.RECON_CONFIG SET ACTIVE_FLAG = 'N' WHERE SRC_TABLE = 'TABLE_NAME_2';


-- =====================================================================
-- PART 3 : RUN
-- =====================================================================
CALL UAT.RUN_RECON();            -- all active pairs
-- CALL UAT.RUN_RECON(2);         -- only RECON_ID = 2


-- =====================================================================
-- PART 4 : RECON REPORTS
-- =====================================================================
SET RUN_ID = (SELECT RUN_ID FROM UAT.DATASTAGE_MIGRATIONS_RECON_REPORT ORDER BY RUN_TS DESC LIMIT 1);

-- R1. Executive summary : matched vs mismatched table counts
WITH t AS (
    SELECT RECON_ID, COUNT_IF(STATUS IN ('FAIL','ERROR')) AS BAD
      FROM UAT.DATASTAGE_MIGRATIONS_RECON_REPORT WHERE RUN_ID = $RUN_ID GROUP BY RECON_ID)
SELECT COUNT(*)              AS TABLES_COMPARED,
       COUNT_IF(BAD = 0)     AS TABLES_MATCHED,
       COUNT_IF(BAD > 0)     AS TABLES_MISMATCHED,
       ROUND(100 * COUNT_IF(BAD = 0) / NULLIF(COUNT(*), 0), 2) AS MATCH_PCT
FROM t;

-- R2. Table-level recon report
SELECT SRC_TABLE,
       TGT_TABLE,
       MAX(IFF(CHECK_NAME = 'ROW_COUNT', SRC_VALUE, NULL))             AS SRC_ROWS,
       MAX(IFF(CHECK_NAME = 'ROW_COUNT', TGT_VALUE, NULL))             AS TGT_ROWS,
       MAX(IFF(CHECK_NAME = 'ROW_COUNT', DIFF_VALUE, NULL))            AS ROW_DIFF,
       MAX(IFF(CHECK_NAME = 'DATA_TYPE_MISMATCH', STATUS, NULL))       AS TYPE_CHECK,
       MAX(IFF(CHECK_NAME = 'FULL_DATA_HASH', STATUS, NULL))           AS DATA_HASH,
       MAX(IFF(CHECK_NAME IN ('KEYS_ONLY_IN_SOURCE','ROWS_ONLY_IN_SOURCE'), DIFF_VALUE, NULL)) AS ONLY_IN_SRC,
       MAX(IFF(CHECK_NAME IN ('KEYS_ONLY_IN_TARGET','ROWS_ONLY_IN_TARGET'), DIFF_VALUE, NULL)) AS ONLY_IN_TGT,
       MAX(IFF(CHECK_NAME = 'KEYS_WITH_VALUE_DIFF', DIFF_VALUE, NULL)) AS VALUE_DIFF_ROWS,
       LISTAGG(IFF(CHECK_NAME LIKE 'COLUMN_HASH:%' AND STATUS = 'FAIL',
                   REPLACE(CHECK_NAME, 'COLUMN_HASH: ', ''), NULL), ', ') AS MISMATCHED_COLUMNS,
       IFF(COUNT_IF(STATUS IN ('FAIL','ERROR')) = 0, 'MATCHED', 'MISMATCH') AS RECON_STATUS
FROM UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
WHERE RUN_ID = $RUN_ID
GROUP BY SRC_TABLE, TGT_TABLE
ORDER BY RECON_STATUS DESC, SRC_TABLE;

-- R3. All failed checks (detail)
SELECT SRC_TABLE, CHECK_NAME, SRC_VALUE, TGT_VALUE, DIFF_VALUE, STATUS
FROM UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
WHERE RUN_ID = $RUN_ID AND STATUS IN ('FAIL','ERROR')
ORDER BY SRC_TABLE, CHECK_NAME;

-- R4. Sample mismatched rows (ONLY_IN_SOURCE vs ONLY_IN_TARGET)
SELECT c.SRC_TABLE, m.SIDE, m.ROW_DATA
FROM UAT.RECON_MISMATCH_SAMPLE m
JOIN UAT.RECON_CONFIG c USING (RECON_ID)
WHERE m.RUN_ID = $RUN_ID
ORDER BY c.SRC_TABLE, m.SIDE;

-- R5. Trend across runs (re-run after fixes and track progress)
SELECT RUN_ID, MIN(RUN_TS) AS RUN_TS,
       COUNT(DISTINCT RECON_ID) AS TABLES,
       COUNT(DISTINCT IFF(STATUS IN ('FAIL','ERROR'), RECON_ID, NULL)) AS TABLES_WITH_ISSUES
FROM UAT.DATASTAGE_MIGRATIONS_RECON_REPORT
GROUP BY RUN_ID
ORDER BY RUN_TS DESC;


-- =====================================================================
-- PART 6 : CLEANUP  (ONLY the objects this script created)
-- ---------------------------------------------------------------------
--  >>> DANGER: this DROPS the recon framework tables and procedure.   <<<
--  >>> Kept commented out so it never runs by accident.               <<<
--  >>> It only removes the RECON objects - it NEVER touches your       <<<
--  >>> source / target business tables.                                <<<
--
--  To clean up: un-comment the lines below (remove the leading --) and run.
-- =====================================================================
-- DROP PROCEDURE IF EXISTS UAT.RUN_RECON(NUMBER);
-- DROP TABLE     IF EXISTS UAT.DATASTAGE_MIGRATIONS_RECON_REPORT;
-- DROP TABLE     IF EXISTS UAT.RECON_MISMATCH_SAMPLE;
-- DROP TABLE     IF EXISTS UAT.RECON_CONFIG;

 
