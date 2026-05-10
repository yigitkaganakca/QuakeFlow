"""Thin MinIO / S3 wrapper used by the backfill container and DAGs.

Storage layout in the bucket:
    <source>/<YYYY-MM-DD>/<HHMMSS>-<uuid>.<ext>

Examples:
    AFAD/2025-01-01/120512-61f....json
    KOERI/2026-05-09/170301-44a....html
"""

from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone
from typing import Iterable, Iterator

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import settings


def _client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access,
        aws_secret_access_key=settings.minio_secret,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        region_name="us-east-1",
    )


def archive_key(source: str, ext: str, when: datetime | None = None) -> str:
    when = when or datetime.now(timezone.utc)
    return (
        f"{source}/{when:%Y-%m-%d}/"
        f"{when:%H%M%S}-{uuid.uuid4().hex[:8]}.{ext}"
    )


def archive_uri(key: str) -> str:
    return f"s3a://{settings.minio_bucket}/{key}"


def put_blob(key: str, blob: bytes, content_type: str = "application/octet-stream") -> str:
    cli = _client()
    cli.put_object(
        Bucket=settings.minio_bucket,
        Key=key,
        Body=blob,
        ContentType=content_type,
    )
    return archive_uri(key)


def get_blob(key: str) -> bytes:
    cli = _client()
    obj = cli.get_object(Bucket=settings.minio_bucket, Key=key)
    return obj["Body"].read()


def list_keys(prefix: str = "") -> Iterator[str]:
    cli = _client()
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.minio_bucket, Prefix=prefix):
        for item in page.get("Contents", []) or []:
            yield item["Key"]


def ensure_bucket() -> None:
    """Idempotent bucket creation. Used as a sanity check on startup."""
    cli = _client()
    try:
        cli.head_bucket(Bucket=settings.minio_bucket)
    except ClientError:
        cli.create_bucket(Bucket=settings.minio_bucket)
