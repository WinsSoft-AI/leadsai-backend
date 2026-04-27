"""
LeadsAI — Storage Service  v1.0
=====================================
Single-mode file storage: AWS S3.

All config from .env — no config.py dependency.
"""
from __future__ import annotations

import os
import logging
import secrets
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Magic bytes for image validation ────────────────────────────────────────
_MAGIC_MAP = {
    b"\xff\xd8\xff":       "image/jpeg",
    b"\x89PNG":            "image/png",
    b"GIF8":               "image/gif",
    b"RIFF":               "image/webp",   # RIFF....WEBP header
    b"<svg":               "image/svg+xml",
}

_EXT_MIME = {
    "jpg": "image/jpeg", "jpeg": "image/jpeg",
    "png": "image/png",  "webp": "image/webp",
    "gif": "image/gif",  "svg": "image/svg+xml",
}

MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB


def validate_image(content: bytes) -> str:
    """
    Validate raw bytes are an actual image via magic-number check.
    Returns the detected MIME type.
    Raises ValueError if not a recognised image format.
    """
    header = content[:8]
    for magic, mime in _MAGIC_MAP.items():
        if header.startswith(magic):
            # Extra check for RIFF-based WEBP: bytes 8-12 must be 'WEBP'
            if magic == b"RIFF" and content[8:12] != b"WEBP":
                continue
            return mime
    raise ValueError("Unrecognised image format — file does not match any known image signature")


def _mime_from_ext(filename: str) -> str:
    """Derive MIME type from file extension, default to application/octet-stream."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return _EXT_MIME.get(ext, "application/octet-stream")


# ═════════════════════════════════════════════════════════════════════════════
# ABSTRACT BASE
# ═════════════════════════════════════════════════════════════════════════════

class StorageService:
    """Interface for file storage — implemented by S3 backend."""

    async def upload(self, key: str, content: bytes, content_type: str,
                     cache_control: str = "public, max-age=31536000, immutable",
                     tagging: str = "") -> str:
        """
        Store a file. Returns the stored key (not the URL).
        """
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        """Delete a single file by key."""
        raise NotImplementedError

    async def delete_prefix(self, prefix: str) -> int:
        """Delete all files under a given key prefix. Returns count of deleted files."""
        raise NotImplementedError

    def get_url(self, key: str, expires_in: int = 3600) -> str:
        """
        Get a URL to access the file.
        - S3 mode: returns a presigned URL (valid for `expires_in` seconds)
        """
        raise NotImplementedError

    def resolve_url(self, stored_value: Optional[str], expires_in: int = 86400) -> str:
        """
        Given a stored DB value (could be an old /uploads/... path, an S3 key, 
        a full URL, or empty), return a usable URL.
        """
        if not stored_value:
            return ""
        # If it's already a full URL (http/https/data:), pass through
        if stored_value.startswith(("http://", "https://", "data:")):
            return stored_value
        # Legacy fallback logic removed
        # Otherwise treat as an S3 key
        return self.get_url(stored_value, expires_in)


# AWS S3 STORAGE
# ═════════════════════════════════════════════════════════════════════════════

class S3StorageService(StorageService):
    """
    Stores files on AWS S3. Bucket is private; all access via presigned URLs.
    """

    def __init__(self):
        import boto3
        self._bucket = os.getenv("S3_BUCKET", "winssoft-bma")
        self._region = os.getenv("S3_REGION", "ap-south-1")
        self._cdn_url = os.getenv("S3_CDN_URL", "").rstrip("/")

        self._client = boto3.client(
            "s3",
            region_name=self._region,
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        )
        logger.info(f"☁️  S3StorageService ready (bucket={self._bucket}, region={self._region})")

    async def upload(self, key: str, content: bytes, content_type: str,
                     cache_control: str = "public, max-age=31536000, immutable",
                     tagging: str = "") -> str:
        import asyncio
        extra = {"ContentType": content_type}
        if cache_control:
            extra["CacheControl"] = cache_control
        if tagging:
            extra["Tagging"] = tagging

        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=content,
            **extra,
        )
        logger.info(f"☁️  Uploaded to S3: {key} ({len(content)} bytes, {content_type})")
        return key

    async def delete(self, key: str) -> None:
        import asyncio
        try:
            await asyncio.to_thread(
                self._client.delete_object,
                Bucket=self._bucket,
                Key=key,
            )
            logger.info(f"☁️  Deleted from S3: {key}")
        except Exception as e:
            logger.warning(f"S3 delete failed for {key}: {e}")

    async def delete_prefix(self, prefix: str) -> int:
        """Delete all objects under a prefix (used for tenant cascade cleanup)."""
        import asyncio
        count = 0
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix)
            for page in pages:
                contents = page.get("Contents", [])
                if not contents:
                    continue
                delete_keys = [{"Key": obj["Key"]} for obj in contents]
                await asyncio.to_thread(
                    self._client.delete_objects,
                    Bucket=self._bucket,
                    Delete={"Objects": delete_keys},
                )
                count += len(delete_keys)
        except Exception as e:
            logger.error(f"S3 delete_prefix failed for {prefix}: {e}")
        logger.info(f"☁️  Deleted {count} S3 objects under prefix: {prefix}")
        return count

    def get_url(self, key: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for private S3 access."""
        if self._cdn_url:
            return f"{self._cdn_url}/{key}"
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def resolve_url(self, stored_value: Optional[str], expires_in: int = 86400) -> str:
        if not stored_value:
            return ""
        # Full URLs pass through
        if stored_value.startswith(("http://", "https://", "data:")):
            return stored_value
        api_url = os.getenv("VITE_API_URL", "").rstrip("/")
        # Legacy /uploads/ path — convert to key
        if stored_value.startswith("/uploads/"):
            key = stored_value.replace("/uploads/", "", 1)
            return f"{api_url}/v1/assets?key={key}"
        # Assume it's an S3 key
        return f"{api_url}/v1/assets?key={stored_value}"


# ═════════════════════════════════════════════════════════════════════════════
# FACTORY — returns the correct backend based on environment
# ═════════════════════════════════════════════════════════════════════════════

def create_storage_service() -> StorageService:
    """
    Factory: returns S3StorageService.
    Raises RuntimeError if credentials are missing.
    """
    s3_key   = os.getenv("S3_ACCESS_KEY", "").strip()
    s3_secret = os.getenv("S3_SECRET_KEY", "").strip()

    if not s3_key or not s3_secret:
        raise RuntimeError("S3_ACCESS_KEY or S3_SECRET_KEY is missing. Local storage fallback is disabled.")

    return S3StorageService()
""", "Description": "New storage service module with S3 and local disk dual-mode support, image validation, and presigned URL generation", "IsArtifact": false, "Overwrite": true, "TargetFile": "d:\\Somesh\\PROJECTS\\OmniChat\\backend\\s3.py"""
