# Deployment Guide (AWS Databricks)

## 1. Prerequisites
* Unity Catalog metastore; permission to create the `dvf_{env}` catalog (or
  have an admin pre-create it).
* Cluster/DBR 13.3+ (liquid clustering for audit tables), shared or
  single-user with UC access.
* S3 bucket for report artifacts (instance profile or UC external location).
* AWS Secrets Manager (or secret-scope backing) for endpoint credentials.

## 2. Build & install the wheel
```bash
pip install build
python -m build                       # dist/dvf-0.1.0-py3-none-any.whl
```
Upload to a UC Volume (or workspace files) and install as a cluster library,
or `%pip install /Volumes/.../dvf-0.1.0-py3-none-any.whl` per notebook.

## 3. Cluster libraries
| Need | Library |
|---|---|
| SQL Server | Maven `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11` |
| Oracle | Maven `com.oracle.database.jdbc:ojdbc11:23.4.0.24.05` |
| PostgreSQL | bundled with DBR (else `org.postgresql:postgresql`) |
| MySQL | Maven `com.mysql:mysql-connector-j:8.4.0` |
| DB2 LUW / z/OS | Maven `com.ibm.db2:jcc:11.5.9.0` (z/OS needs a DB2 Connect license jar from IBM) |
| Teradata | Maven `com.teradata.jdbc:terajdbc:20.00.00.10` |
| Snowflake | PyPI `snowflake-connector-python`; Spark connector bundled with DBR |
| COBOL/mainframe | Maven `za.co.absa.cobrix:spark-cobol_2.12:2.7.3` |
| XML files | Maven `com.databricks:spark-xml_2.12:0.18.0` (native in DBR 14.3+) |
| Excel files | Maven `com.crealytics:spark-excel_2.12:3.5.0_0.20.3` |
| PDF reports (optional) | PyPI `xhtml2pdf` |

## 4. Secrets
```bash
databricks secrets create-scope dvf-prod   # or AWS-SM-backed scope
databricks secrets put-secret dvf-prod snowflake-user
databricks secrets put-secret dvf-prod snowflake-password
```
AWS Secrets Manager alternative: store `{"username": "...", "password": "..."}`
and reference the ARN in `meta.connections.aws_secret_arn`. Grant the cluster
instance profile `secretsmanager:GetSecretValue` + SES `ses:SendRawEmail`
(email) + S3 rw on the report bucket.

## 5. Configure
Edit `config/environments/{env}.yaml`: `catalog`, `secret_scope_default`,
`reporting.output_path` (S3). Global behavior in `config/framework.yaml`.
Anything is overridable per job via env vars: `DVF__ENGINE__MAX_PARALLEL=16`.

## 6. Unity Catalog grants
```sql
CREATE CATALOG IF NOT EXISTS dvf_prod;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG dvf_prod TO `dq-analysts`;
GRANT ALL PRIVILEGES ON CATALOG dvf_prod TO `dvf-service-principal`;
```
Source/target systems need only **read** accounts for the validation user.

## 7. Bootstrap & verify
1. `00_bootstrap_metadata` (env widget) — idempotent DDL + profile seeds.
2. `09_integration_smoke_test` — must print `SMOKE TEST PASSED`.

## 8. Jobs (Databricks Asset Bundle)
`databricks.yml` at the repo root defines the bundle: nightly validation run
(notebook 10), monthly retention (notebook 11). Deploy:
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run dvf_nightly_validation -t dev
```
Targets `dev`/`prod` map to the matching config environment. Adjust cluster
node types, schedules, and groups in `databricks.yml`.

## 9. Upgrades
Rebuild the wheel, update the cluster library, rerun `00_bootstrap_metadata`
(CREATE IF NOT EXISTS — existing data untouched). New columns on framework
tables ship with explicit migration notes in release notes.
