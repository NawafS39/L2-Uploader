import io
import gzip
import datetime as dt
from typing import List, Dict, Any

import boto3
from aws_config import AWSConfig


class S3Uploader:
    def __init__(self, aws_config: AWSConfig):
        self.c = aws_config
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.c.aws_access_key_id,
            aws_secret_access_key=self.c.aws_secret_access_key,
            region_name=self.c.aws_region,
        )

    def _build_key(self, exchange: str, stream_name: str, ts: dt.datetime) -> str:
        date_path = ts.strftime("%Y/%m/%d/%H")
        stamp = ts.strftime("%Y%m%dT%H%M%S")
        return f"{self.c.s3_prefix}/{exchange}/{stream_name}/{date_path}/{stamp}.jsonl.gz"

    def upload_messages(self, exchange: str, stream_name: str, msgs: List[Dict[str, Any]], ts: dt.datetime) -> str:
        if not msgs:
            return ""

        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="w") as gz:
            for msg in msgs:
                gz.write(msg["_raw"].encode("utf-8") + b"\n")
        buf.seek(0)

        key = self._build_key(exchange, stream_name, ts)
        self.s3.upload_fileobj(buf, self.c.s3_bucket_name, key)
        return key
