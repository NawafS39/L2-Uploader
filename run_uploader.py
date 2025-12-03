import asyncio
from aws_config import AWSConfig, StreamConfig
from binance_l2_stream import BinanceL2Streamer

async def main():
    aws = AWSConfig()
    stream = StreamConfig()

    print(f"[BOOT] Stream → {stream.stream_name}")
    print(f"[BOOT] Bucket → {aws.s3_bucket_name}")

    client = BinanceL2Streamer(aws, stream)
    await client.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Exited by user")
