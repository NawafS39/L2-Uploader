#!/usr/bin/env python3
"""
Binance Futures historical klines downloader.

Downloads monthly klines ZIP files from Binance Vision, optionally extracts
to CSV, and converts to Parquet format.
"""

import argparse
import logging
import sys
import time
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# Optional boto3 import for S3 uploads
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None  # type: ignore
    ClientError = Exception  # type: ignore
    NoCredentialsError = Exception  # type: ignore

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

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


def build_binance_url(symbol: str, interval: str, year: int, month: int) -> str:
    """Build the Binance Vision URL for a monthly klines ZIP file."""
    filename = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    return f"{BASE_URL}/{symbol}/{interval}/{filename}"


def build_local_paths(
    data_dir: str, symbol: str, interval: str, year: int, month: int
) -> tuple[Path, Path, Path]:
    """Build local file paths for ZIP, CSV, and Parquet files."""
    base_dir = Path(data_dir) / symbol / interval / str(year)
    filename_base = f"{symbol}-{interval}-{year}-{month:02d}"
    zip_path = base_dir / f"{filename_base}.zip"
    csv_path = base_dir / f"{filename_base}.csv"
    parquet_path = base_dir / f"{filename_base}.parquet"
    return zip_path, csv_path, parquet_path


def download_file(
    url: str, output_path: Path, timeout: float, overwrite: bool = False
) -> bool:
    """
    Download a file from URL to disk using streaming.

    Returns True if successful, False otherwise.
    """
    if output_path.exists() and not overwrite:
        logger.info(f"Skipping download (exists): {output_path.name}")
        return True

    try:
        logger.info(f"Downloading: {url}")
        response = requests.get(url, stream=True, timeout=timeout)

        if response.status_code != 200:
            logger.warning(
                f"HTTP {response.status_code} for {url} - skipping"
            )
            return False

        # Create parent directories
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Stream download in chunks
        chunk_size = 2 * 1024 * 1024  # 2 MB chunks
        total_size = 0
        chunk_count = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
                    chunk_count += 1
                    # Log progress every 10 chunks (~20 MB)
                    if chunk_count % 10 == 0:
                        logger.debug(
                            f"Downloaded {total_size / (1024 * 1024):.1f} MB"
                        )

        logger.info(f"Downloaded: {output_path.name} ({total_size / (1024 * 1024):.1f} MB)")
        return True

    except requests.exceptions.Timeout:
        logger.error(f"Timeout downloading {url}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error downloading {url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}", exc_info=logger.isEnabledFor(logging.DEBUG))
        return False


def extract_zip(zip_path: Path, output_dir: Path, overwrite: bool = False) -> Optional[Path]:
    """
    Extract a ZIP file to the output directory.

    Returns the path to the extracted CSV file, or None if extraction failed.
    """
    if not zip_path.exists():
        logger.warning(f"ZIP file not found: {zip_path}")
        return None

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Get the list of files in the ZIP
            file_list = zip_ref.namelist()
            if not file_list:
                logger.warning(f"ZIP file is empty: {zip_path}")
                return None

            # Binance ZIPs contain a single CSV file
            csv_filename = file_list[0]
            csv_path = output_dir / csv_filename

            if csv_path.exists() and not overwrite:
                logger.info(f"Skipping extraction (CSV exists): {csv_path.name}")
                return csv_path

            # Extract to the same directory as the ZIP
            logger.info(f"Extracting: {zip_path.name} -> {csv_path.name}")
            zip_ref.extractall(output_dir)

            if csv_path.exists():
                logger.info(f"Extracted: {csv_path.name}")
                return csv_path
            else:
                logger.warning(f"Expected CSV not found after extraction: {csv_path}")
                return None

    except zipfile.BadZipFile:
        logger.error(f"Invalid ZIP file: {zip_path}")
        return None
    except Exception as e:
        logger.error(
            f"Error extracting {zip_path}: {e}",
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        return None


def get_s3_client():
    """
    Lazy initialization of S3 client.

    Returns boto3 S3 client or None if boto3 is not available.
    """
    if not BOTO3_AVAILABLE:
        return None
    try:
        return boto3.client("s3")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        return None


def build_s3_key(s3_prefix: str, symbol: str, interval: str, year: int, filename: str) -> str:
    """Build S3 key path: {prefix}/{symbol}/{interval}/{year}/{filename}"""
    return f"{s3_prefix}/{symbol}/{interval}/{year}/{filename}"


def upload_to_s3(
    local_path: Path, bucket: str, key: str, s3_client
) -> bool:
    """
    Upload a local file to S3.

    Returns True if successful, False otherwise.
    """
    if not local_path.exists():
        logger.warning(f"File not found for S3 upload: {local_path}")
        return False

    if s3_client is None:
        return False

    try:
        logger.info(f"Uploading to S3: s3://{bucket}/{key}")
        s3_client.upload_file(str(local_path), bucket, key)
        logger.info(f"Uploaded to S3: s3://{bucket}/{key}")
        return True
    except NoCredentialsError:
        logger.warning(f"S3 upload failed: AWS credentials not found")
        return False
    except ClientError as e:
        logger.warning(f"S3 upload failed for {key}: {e}")
        return False
    except Exception as e:
        logger.warning(
            f"Unexpected error uploading {local_path} to S3: {e}",
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        return False


def convert_to_parquet(
    csv_path: Path, parquet_path: Path, overwrite: bool = False, keep_csv: bool = False
) -> bool:
    """
    Convert a CSV file to Parquet format.

    Returns True if successful, False otherwise.
    """
    if not csv_path.exists():
        logger.warning(f"CSV file not found: {csv_path}")
        return False

    if parquet_path.exists() and not overwrite:
        logger.info(f"Skipping conversion (Parquet exists): {parquet_path.name}")
        return True

    try:
        logger.info(f"Converting to Parquet: {csv_path.name} -> {parquet_path.name}")

        # Read CSV with pandas (let it infer types)
        df = pd.read_csv(csv_path)

        # Write Parquet using pyarrow engine if available
        try:
            df.to_parquet(parquet_path, engine="pyarrow", index=False)
        except ImportError:
            # Fallback to fastparquet if pyarrow not available
            logger.warning("pyarrow not available, trying fastparquet")
            df.to_parquet(parquet_path, engine="fastparquet", index=False)

        logger.info(f"Created Parquet: {parquet_path.name}")

        # Delete CSV if requested
        if not keep_csv:
            csv_path.unlink()
            logger.info(f"Deleted CSV: {csv_path.name}")

        return True

    except Exception as e:
        logger.error(
            f"Error converting {csv_path} to Parquet: {e}",
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        return False


def process_symbol_year_month(
    symbol: str,
    interval: str,
    year: int,
    month: int,
    data_dir: str,
    timeout: float,
    sleep: float,
    overwrite: bool,
    extract: bool,
    to_parquet: bool,
    keep_csv: bool,
    s3_bucket: Optional[str] = None,
    s3_prefix: Optional[str] = None,
    s3_client=None,
) -> None:
    """Process a single symbol/year/month combination."""
    url = build_binance_url(symbol, interval, year, month)
    zip_path, csv_path, parquet_path = build_local_paths(
        data_dir, symbol, interval, year, month
    )

    logger.info(f"Processing {symbol} {year}-{month:02d}")

    # Download
    download_success = download_file(url, zip_path, timeout, overwrite)
    if not download_success:
        return

    # Sleep after download for rate limiting
    time.sleep(sleep)

    # Extract if requested
    if extract and download_success:
        csv_path_result = extract_zip(zip_path, zip_path.parent, overwrite)
        if csv_path_result:
            csv_path = csv_path_result

            # Convert to Parquet if requested
            if to_parquet:
                parquet_created = convert_to_parquet(csv_path, parquet_path, overwrite, keep_csv)
                
                # Upload to S3 if requested and Parquet was created or already exists
                if s3_bucket and s3_prefix and s3_client and parquet_path.exists():
                    s3_key = build_s3_key(s3_prefix, symbol, interval, year, parquet_path.name)
                    upload_to_s3(parquet_path, s3_bucket, s3_key, s3_client)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download Binance Futures historical klines data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Trading symbols (e.g., BTCUSDT ETHUSDT SOLUSDT)",
    )

    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        required=True,
        help="Years to download (e.g., 2023 2024 2025)",
    )

    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="Kline interval (default: 1m)",
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/binance_klines",
        help="Root directory for downloaded files (default: data/binance_klines)",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files",
    )

    parser.add_argument(
        "--extract",
        action="store_true",
        help="Extract ZIP files to CSV",
    )

    parser.add_argument(
        "--to-parquet",
        action="store_true",
        help="Convert CSV files to Parquet format",
    )

    parser.add_argument(
        "--keep-csv",
        action="store_true",
        help="Keep CSV files after converting to Parquet (only relevant with --to-parquet)",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds (default: 60)",
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep time in seconds between downloads (default: 0.2)",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging with full stack traces",
    )

    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=None,
        help="S3 bucket name for uploading Parquet files (optional)",
    )

    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="binance_klines",
        help="S3 key prefix (default: binance_klines)",
    )

    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Disable S3 uploads even if --s3-bucket is provided",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    setup_logging(debug=args.debug)

    # Validate arguments
    if args.to_parquet and not args.extract:
        logger.warning(
            "--to-parquet requires --extract. Enabling --extract automatically."
        )
        args.extract = True

    # Setup S3 if requested
    s3_bucket = None
    s3_prefix = None
    s3_client = None

    if args.s3_bucket and not args.no_s3:
        if not BOTO3_AVAILABLE:
            logger.error(
                "boto3 is not available. Install it with: pip install boto3. "
                "S3 uploads will be skipped."
            )
        else:
            s3_bucket = args.s3_bucket
            s3_prefix = args.s3_prefix
            s3_client = get_s3_client()
            if s3_client:
                logger.info(f"S3 upload enabled: s3://{s3_bucket}/{s3_prefix}/")
            else:
                logger.warning("S3 client initialization failed. S3 uploads will be skipped.")
    elif args.no_s3:
        logger.info("S3 uploads disabled (--no-s3 flag set)")

    # Process all combinations
    total_tasks = len(args.symbols) * len(args.years) * 12
    logger.info(
        f"Starting download: {len(args.symbols)} symbol(s), "
        f"{len(args.years)} year(s), 12 months each = {total_tasks} tasks"
    )

    processed = 0
    for symbol in args.symbols:
        symbol_upper = symbol.upper()
        for year in args.years:
            for month in range(1, 13):
                try:
                    process_symbol_year_month(
                        symbol=symbol_upper,
                        interval=args.interval,
                        year=year,
                        month=month,
                        data_dir=args.data_dir,
                        timeout=args.timeout,
                        sleep=args.sleep,
                        overwrite=args.overwrite,
                        extract=args.extract,
                        to_parquet=args.to_parquet,
                        keep_csv=args.keep_csv,
                        s3_bucket=s3_bucket,
                        s3_prefix=s3_prefix,
                        s3_client=s3_client,
                    )
                    processed += 1
                except KeyboardInterrupt:
                    logger.info("Interrupted by user")
                    sys.exit(1)
                except Exception as e:
                    logger.error(
                        f"Unexpected error processing {symbol_upper} {year}-{month:02d}: {e}",
                        exc_info=logger.isEnabledFor(logging.DEBUG),
                    )
                    # Continue with next month
                    continue

    logger.info(f"Completed: processed {processed}/{total_tasks} tasks")


if __name__ == "__main__":
    main()

