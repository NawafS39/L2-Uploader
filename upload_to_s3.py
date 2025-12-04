import io
import gzip
import datetime as dt
import logging
import asyncio
from typing import List, Dict, Any
from functools import partial

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from aws_config import AWSConfig

logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self, aws_config: AWSConfig):
        self.c = aws_config
        try:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=self.c.aws_access_key_id,
                aws_secret_access_key=self.c.aws_secret_access_key,
                region_name=self.c.aws_region,
            )
            # Test credentials with a simple operation
            logger.info(f"S3 client initialized for region: {self.c.aws_region}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}", exc_info=True)
            raise

    def _build_key(self, exchange: str, stream_name: str, ts: dt.datetime) -> str:
        date_path = ts.strftime("%Y/%m/%d/%H")
        stamp = ts.strftime("%Y%m%dT%H%M%S")
        return f"{self.c.s3_prefix}/{exchange}/{stream_name}/{date_path}/{stamp}.jsonl.gz"

    def _upload_sync(self, buf: io.BytesIO, bucket: str, key: str) -> None:
        """Synchronous upload function to run in executor."""
        try:
            self.s3.upload_fileobj(buf, bucket, key)
            logger.debug(f"S3 upload completed: {key}")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.error(f"S3 ClientError ({error_code}) uploading {key}: {e}")
            raise
        except BotoCoreError as e:
            logger.error(f"S3 BotoCoreError uploading {key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading {key}: {e}", exc_info=True)
            raise

    async def upload_messages(
        self, exchange: str, stream_name: str, msgs: List[Dict[str, Any]], ts: dt.datetime, max_retries: int = 3
    ) -> str:
        """Upload messages to S3 with async execution and retry logic."""
        if not msgs:
            return ""

        # Prepare buffer synchronously (fast operation)
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="w") as gz:
            for msg in msgs:
                gz.write(msg["_raw"].encode("utf-8") + b"\n")
        buf.seek(0)

        key = self._build_key(exchange, stream_name, ts)
        
        # Retry logic with exponential backoff
        last_exception = None
        for attempt in range(max_retries):
            try:
                # Run blocking S3 upload in thread pool to avoid blocking event loop
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, partial(self._upload_sync, buf, self.c.s3_bucket_name, key)
                )
                return key
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.warning(
                        f"Upload attempt {attempt + 1}/{max_retries} failed for {key}, retrying in {wait_time}s: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    # Reset buffer position for retry
                    buf.seek(0)
                else:
                    logger.error(f"All {max_retries} upload attempts failed for {key}")
        
        # If all retries failed, raise the last exception
        raise last_exception
