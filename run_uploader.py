import asyncio
import logging
import sys
from pathlib import Path
from aws_config import AWSConfig, StreamConfig
from binance_l2_stream import BinanceL2Streamer

# Configure logging
def setup_logging(log_file: str = None, log_level: str = "INFO"):
    """Setup logging with both file and console handlers."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # More verbose in file
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger

async def main():
    logger = logging.getLogger(__name__)
    
    try:
        aws = AWSConfig()
        stream = StreamConfig()

        logger.info("=" * 60)
        logger.info("L2-Uploader Starting")
        logger.info("=" * 60)
        logger.info(f"Stream: {stream.stream_name}")
        logger.info(f"S3 Bucket: {aws.s3_bucket_name}")
        logger.info(f"S3 Prefix: {aws.s3_prefix}")
        logger.info(f"Batch Config: {stream.batch_seconds}s or {stream.max_messages_per_batch} messages")
        logger.info("=" * 60)

        # Validate configuration
        if not aws.aws_access_key_id or not aws.aws_secret_access_key:
            logger.error("AWS credentials not configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
            sys.exit(1)
        
        if not aws.s3_bucket_name:
            logger.error("S3 bucket not configured (S3_BUCKET_NAME)")
            sys.exit(1)

        client = BinanceL2Streamer(aws, stream)
        await client.run()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    import os
    
    # Setup logging from environment or defaults
    log_file = os.getenv("LOG_FILE", "logs/l2_uploader.log")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    
    setup_logging(log_file=log_file, log_level=log_level)
    logger = logging.getLogger(__name__)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
