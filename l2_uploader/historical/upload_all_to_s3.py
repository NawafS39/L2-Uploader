#!/usr/bin/env python3
"""
Upload all local Parquet files to S3.

Recursively scans a directory for .parquet files and uploads them to S3,
preserving the folder structure.
"""

import argparse
import logging
import os
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    """Configure logging format and level."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_s3_client():
    """
    Initialize boto3 S3 client using credentials from .env file.

    Returns boto3 S3 client or None if boto3 is not available.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
    except ImportError:
        logger.error("boto3 is not available. Install it with: pip install boto3")
        return None

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if not aws_access_key_id or not aws_secret_access_key:
        logger.error(
            "AWS credentials not found in .env file. "
            "Required: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"
        )
        return None

    try:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        s3 = session.client("s3")
        logger.info(f"S3 client initialized for region: {aws_region}")
        return s3
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        return None


def find_parquet_files(data_dir: Path) -> list[Path]:
    """
    Recursively find all .parquet files in the data directory.

    Returns a list of Path objects for all .parquet files found.
    """
    parquet_files = []
    if not data_dir.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return parquet_files

    if not data_dir.is_dir():
        logger.warning(f"Data directory is not a directory: {data_dir}")
        return parquet_files

    for file_path in data_dir.rglob("*.parquet"):
        if file_path.is_file():
            parquet_files.append(file_path)

    return sorted(parquet_files)


def build_s3_key(local_path: Path, data_dir: Path, s3_prefix: str) -> str:
    """
    Build S3 key from local file path, preserving folder structure.

    Example:
        Local: data/BTCUSDT/1m/2024/BTCUSDT-1m-2024-01.parquet
        S3 key: binance_klines/BTCUSDT/1m/2024/BTCUSDT-1m-2024-01.parquet
    """
    # Get relative path from data_dir
    try:
        relative_path = local_path.relative_to(data_dir)
    except ValueError:
        # If local_path is not relative to data_dir, use the full path
        logger.warning(
            f"File {local_path} is not relative to data_dir {data_dir}. "
            "Using full path structure."
        )
        relative_path = local_path

    # Convert to string and normalize path separators
    relative_str = str(relative_path).replace("\\", "/")

    # Build S3 key
    s3_key = f"{s3_prefix}/{relative_str}" if s3_prefix else relative_str

    return s3_key


def upload_file_to_s3(
    local_path: Path, bucket: str, s3_key: str, s3_client, dry_run: bool = False
) -> Tuple[bool, str]:
    """
    Upload a single file to S3.

    Returns (success: bool, message: str)
    """
    if not local_path.exists():
        return False, f"File not found: {local_path}"

    if s3_client is None:
        return False, "S3 client not available"

    if dry_run:
        file_size = local_path.stat().st_size / (1024 * 1024)  # MB
        return True, f"DRY RUN: Would upload {file_size:.2f} MB"

    try:
        logger.info(f"Uploading {local_path} → s3://{bucket}/{s3_key}")
        s3_client.upload_file(str(local_path), bucket, s3_key)
        file_size = local_path.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"Uploaded successfully ({file_size:.2f} MB)")
        return True, "Uploaded successfully"
    except Exception as e:
        error_msg = f"Upload failed: {e}"
        logger.warning(error_msg)
        return False, error_msg


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload all local Parquet files to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Local directory containing Parquet files (default: ./data)",
    )

    parser.add_argument(
        "--s3-bucket",
        type=str,
        required=True,
        help="S3 bucket name (required)",
    )

    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="binance_klines",
        help="S3 key prefix (default: binance_klines)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be uploaded without actually uploading",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    setup_logging(debug=args.debug)

    # Convert data_dir to Path
    data_dir = Path(args.data_dir).resolve()

    logger.info(f"Scanning for Parquet files in: {data_dir}")
    logger.info(f"S3 bucket: {args.s3_bucket}")
    logger.info(f"S3 prefix: {args.s3_prefix}")
    if args.dry_run:
        logger.info("DRY RUN mode: no files will be uploaded")

    # Find all Parquet files
    parquet_files = find_parquet_files(data_dir)
    total_files = len(parquet_files)

    if total_files == 0:
        logger.warning(f"No Parquet files found in {data_dir}")
        return

    logger.info(f"Found {total_files} Parquet file(s)")

    # Initialize S3 client (unless dry-run, we still need it to check credentials)
    s3_client = None
    if not args.dry_run:
        s3_client = get_s3_client()
        if s3_client is None:
            logger.error("Cannot proceed without S3 client. Exiting.")
            return
    else:
        # For dry-run, we still try to get credentials to validate them
        s3_client = get_s3_client()
        if s3_client is None:
            logger.warning(
                "S3 client initialization failed, but continuing in dry-run mode"
            )

    # Upload files
    uploaded_count = 0
    failed_count = 0
    skipped_count = 0

    for local_path in parquet_files:
        s3_key = build_s3_key(local_path, data_dir, args.s3_prefix)
        success, message = upload_file_to_s3(
            local_path, args.s3_bucket, s3_key, s3_client, dry_run=args.dry_run
        )

        if success:
            if args.dry_run:
                skipped_count += 1
            else:
                uploaded_count += 1
        else:
            failed_count += 1

    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files found: {total_files}")
    if args.dry_run:
        logger.info(f"Would upload: {skipped_count}")
        logger.info(f"Would fail: {failed_count}")
    else:
        logger.info(f"Successfully uploaded: {uploaded_count}")
        logger.info(f"Failed: {failed_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

