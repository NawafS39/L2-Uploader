import os
from dotenv import load_dotenv

load_dotenv()  # load from .env if present

class AWSConfig:
    def __init__(self) -> None:
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_REGION", "eu-central-1")
        self.s3_bucket_name = os.getenv("S3_BUCKET_NAME")
        self.s3_prefix = os.getenv("S3_PREFIX", "binance/l2")

class StreamConfig:
    def __init__(self) -> None:
        self.symbol = os.getenv("SYMBOL", "btcusdt").lower()
        self.depth_level = int(os.getenv("DEPTH_LEVEL", "20"))
        self.update_speed_ms = int(os.getenv("UPDATE_SPEED_MS", "1000"))
        self.batch_seconds = int(os.getenv("BATCH_SECONDS", "10"))
        self.max_messages_per_batch = int(os.getenv("MAX_MESSAGES_PER_BATCH", "5000"))

    @property
    def stream_name(self) -> str:
        return f"{self.symbol}@depth{self.depth_level}@{self.update_speed_ms}ms"
