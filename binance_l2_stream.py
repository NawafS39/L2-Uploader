import asyncio
import datetime as dt
import time
from typing import List, Dict, Any

import websockets

from aws_config import AWSConfig, StreamConfig
from upload_to_s3 import S3Uploader


BINANCE_WS = "wss://stream.binance.com:9443/stream"


class BinanceL2Streamer:
    def __init__(self, aws_config: AWSConfig, stream_config: StreamConfig):
        self.aws = aws_config
        self.conf = stream_config
        self.uploader = S3Uploader(aws_config)

        self.exchange = "binance"
        self.stream_name = stream_config.stream_name

        self.batch: List[Dict[str, Any]] = []
        self.batch_start = time.time()

    async def connect_loop(self):
        url = f"{BINANCE_WS}?streams={self.stream_name}"
        print(f"[INFO] Connecting to {url}")

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    print("[INFO] Connected.")
                    await self._recv_loop(ws)
            except Exception as e:
                print(f"[ERROR] WebSocket error: {e}")
                print("[INFO] Reconnecting in 5s…")
                await asyncio.sleep(5)

    async def _recv_loop(self, ws):
        while True:
            msg = await ws.recv()
            ts = dt.datetime.utcnow()

            self.batch.append({"_raw": msg, "_ts": ts.isoformat()})

            if (
                time.time() - self.batch_start >= self.conf.batch_seconds
                or len(self.batch) >= self.conf.max_messages_per_batch
            ):
                await self._flush()

    async def _flush(self):
        if not self.batch:
            return

        ts = dt.datetime.utcnow()
        count = len(self.batch)

        print(f"[INFO] Flushing {count} msgs…")

        try:
            key = self.uploader.upload_messages(
                self.exchange, self.stream_name, self.batch, ts
            )
            print(f"[INFO] Uploaded → s3://{self.aws.s3_bucket_name}/{key}")
        except Exception as e:
            print(f"[ERROR] Upload failed: {e}")

        self.batch = []
        self.batch_start = time.time()

    async def run(self):
        await self.connect_loop()
