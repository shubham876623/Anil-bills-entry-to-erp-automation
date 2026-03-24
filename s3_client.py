"""
Upload PDFs to AWS S3. Key format: Bill_Images/<timestamp>~<filename>.
Skipped if S3 credentials are not configured in .env.
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

S3_FOLDER = "Bill_Images"

# Cache boto3 client across uploads within the same process
_s3_client = None


def _get_env(key: str, default: str = "") -> str:
    return (os.environ.get(key) or default).strip()


def is_s3_configured() -> bool:
    return bool(_get_env("S3_BUCKET_NAME") and _get_env("AWS_ACCESS_KEY_ID") and _get_env("AWS_SECRET_ACCESS_KEY"))


def get_s3_object_url(key: str) -> Optional[str]:
    """Build the public S3 URL for a key. Returns None if not configured."""
    if not key or not is_s3_configured():
        return None
    bucket = _get_env("S3_BUCKET_NAME")
    region = _get_env("AWS_REGION") or _get_env("AWS_DEFAULT_REGION") or "us-east-1"
    return f"https://{bucket}.s3.{region}.amazonaws.com/{quote(key, safe='/~')}"


def _get_s3_client():
    """Return a cached boto3 S3 client."""
    global _s3_client
    if _s3_client is None:
        import boto3
        region = _get_env("AWS_REGION") or _get_env("AWS_DEFAULT_REGION") or "us-east-1"
        _s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=_get_env("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=_get_env("AWS_SECRET_ACCESS_KEY"),
        )
    return _s3_client


def upload_pdf_to_s3(
    pdf_path: Path,
    key: Optional[str] = None,
    original_filename: Optional[str] = None,
    content_type: str = "application/pdf",
) -> Optional[str]:
    """Upload PDF to S3. Returns the S3 key on success, None if skipped/failed."""
    if not is_s3_configured():
        return None
    path = Path(pdf_path)
    if not path.is_file():
        return None

    if key is None:
        filename = (original_filename or path.name).strip() or path.name
        filename = filename.replace(" ", "-").replace(",", "-").replace("+", "-")
        ts = datetime.now().strftime("%d%m%y%H%M%S")
        key = f"{S3_FOLDER}/{ts}~{filename}"

    try:
        extra = {"ContentType": content_type}
        if _get_env("S3_PUBLIC_READ", "1").lower() in ("1", "true", "yes"):
            extra["ACL"] = "public-read"
        _get_s3_client().upload_file(str(path), _get_env("S3_BUCKET_NAME"), key, ExtraArgs=extra)
        return key
    except Exception:
        return None
