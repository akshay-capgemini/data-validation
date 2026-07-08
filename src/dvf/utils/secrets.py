"""SecretResolver — credentials only ever live in memory, sourced from:

* DATABRICKS_SCOPE      dbutils.secrets.get(scope, key)  (scopes may be AWS-SM backed)
* AWS_SECRETS_MANAGER   boto3 get_secret_value(arn) -> JSON {username, password}
* ENV                   DVF_SECRET_{SCOPE}_{KEY} env vars (local/dev only)
* NONE                  no credentials (files, instance-profile access, sqlite)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from dvf.core.exceptions import SecretResolutionError
from dvf.metadata.models import AuthMethods, ConnectionDef


@dataclass(frozen=True)
class Credentials:
    user: str | None = None
    password: str | None = None

    def __repr__(self) -> str:  # never leak values into logs/tracebacks
        return f"Credentials(user={'***' if self.user else None}, password={'***' if self.password else None})"


class SecretResolver:
    def __init__(self, dbutils=None, boto3_session=None):
        self._dbutils = dbutils
        self._boto3_session = boto3_session

    def resolve(self, conn: ConnectionDef) -> Credentials:
        method = (conn.auth_method or AuthMethods.NONE).upper()
        if method == AuthMethods.NONE:
            return Credentials()
        if method == AuthMethods.DATABRICKS_SCOPE:
            return self._from_databricks_scope(conn)
        if method == AuthMethods.AWS_SECRETS_MANAGER:
            return self._from_aws_secrets_manager(conn)
        if method == AuthMethods.ENV:
            return self._from_env(conn)
        raise SecretResolutionError(
            f"Unknown auth_method '{conn.auth_method}' on connection '{conn.connection_name}'")

    # ------------------------------------------------------------------ #
    def _from_databricks_scope(self, conn: ConnectionDef) -> Credentials:
        dbutils = self._dbutils
        if dbutils is None:
            from dvf.utils.spark import get_dbutils
            dbutils = get_dbutils()
        if dbutils is None:
            raise SecretResolutionError(
                f"Connection '{conn.connection_name}' uses DATABRICKS_SCOPE but dbutils "
                "is unavailable (not running on Databricks?)")
        if not conn.secret_scope:
            raise SecretResolutionError(
                f"Connection '{conn.connection_name}': secret_scope is required")
        try:
            user = dbutils.secrets.get(conn.secret_scope, conn.secret_key_user) \
                if conn.secret_key_user else None
            password = dbutils.secrets.get(conn.secret_scope, conn.secret_key_password) \
                if conn.secret_key_password else None
        except Exception as exc:
            raise SecretResolutionError(
                f"Secret lookup failed for connection '{conn.connection_name}' "
                f"(scope '{conn.secret_scope}'): {exc}") from exc
        return Credentials(user=user, password=password)

    def _from_aws_secrets_manager(self, conn: ConnectionDef) -> Credentials:
        if not conn.aws_secret_arn:
            raise SecretResolutionError(
                f"Connection '{conn.connection_name}': aws_secret_arn is required")
        try:
            import boto3  # lazy: optional dependency
        except ImportError as exc:  # pragma: no cover
            raise SecretResolutionError("boto3 is not installed (pip install 'dvf[aws]')") from exc
        session = self._boto3_session or boto3
        try:
            client = session.client("secretsmanager")
            payload = client.get_secret_value(SecretId=conn.aws_secret_arn)["SecretString"]
            data = json.loads(payload)
        except Exception as exc:
            raise SecretResolutionError(
                f"AWS Secrets Manager lookup failed for '{conn.connection_name}': {exc}") from exc
        return Credentials(user=data.get("username") or data.get("user"),
                           password=data.get("password"))

    def _from_env(self, conn: ConnectionDef) -> Credentials:
        scope = (conn.secret_scope or "DVF").upper().replace("-", "_")

        def read(key: str | None) -> str | None:
            if not key:
                return None
            name = f"DVF_SECRET_{scope}_{key.upper().replace('-', '_')}"
            value = os.environ.get(name)
            if value is None:
                raise SecretResolutionError(
                    f"Env secret '{name}' not set (connection '{conn.connection_name}')")
            return value

        return Credentials(user=read(conn.secret_key_user),
                           password=read(conn.secret_key_password))
