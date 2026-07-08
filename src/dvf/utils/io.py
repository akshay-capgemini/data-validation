"""Output helpers — write report artifacts to local paths, DBFS, or S3.

Local paths use plain file I/O. dbfs:/ and s3:// (or s3a://) go through dbutils
when on Databricks; s3 falls back to boto3 off-cluster.
"""

from __future__ import annotations

from pathlib import Path

from dvf.core.exceptions import ReportError

_REMOTE_PREFIXES = ("dbfs:/", "s3://", "s3a://", "abfss://")


def is_remote(path: str) -> bool:
    return str(path).startswith(_REMOTE_PREFIXES)


def join_path(base: str, *parts: str) -> str:
    base = str(base)
    if is_remote(base):
        return "/".join([base.rstrip("/")] + [p.strip("/") for p in parts])
    return str(Path(base).joinpath(*parts))


def write_text(path: str, content: str, encoding: str = "utf-8") -> str:
    return write_bytes(path, content.encode(encoding))


def write_bytes(path: str, content: bytes) -> str:
    path = str(path)
    if not is_remote(path):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
        return str(p)

    from dvf.utils.spark import get_dbutils
    dbutils = get_dbutils()
    if dbutils is not None:
        try:
            dbutils.fs.put(path, content.decode("utf-8", errors="replace"), overwrite=True)
            return path
        except Exception:
            local = "/tmp/dvf_report_tmp.bin"
            Path(local).write_bytes(content)
            dbutils.fs.cp(f"file:{local}", path, recurse=False)
            return path

    if path.startswith(("s3://", "s3a://")):
        try:
            import boto3  # lazy: optional dependency
        except ImportError as exc:
            raise ReportError(f"Cannot write '{path}': boto3 not installed and no dbutils") from exc
        without = path.split("://", 1)[1]
        bucket, _, key = without.partition("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content)
        return path

    raise ReportError(f"No writer available for remote path '{path}'")
