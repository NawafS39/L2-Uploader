import asyncio
import datetime as dt
import time
import logging
import signal
from typing import List, Dict, Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from aws_config import AWSConfig, StreamConfig
from upload_to_s3 import S3Uploader

logger = logging.getLogger(__name__)

BINANCE_WS = "wss://stream.binance.com:9443/stream"
MAX_RECONNECT_DELAY = 300  # 5 minutes max delay
INITIAL_RECONNECT_DELAY = 1  # Start with 1 second


class BinanceL2Streamer:
    def __init__(self, aws_config: AWSConfig, stream_config: StreamConfig):
        self.aws = aws_config
        self.conf = stream_config
        self.uploader = S3Uploader(aws_config)

        self.exchange = "binance"
        self.stream_name = stream_config.stream_name

        self.batch: List[Dict[str, Any]] = []
        self.batch_start = time.time()
        self.reconnect_delay = INITIAL_RECONNECT_DELAY
        self.running = True
        self._shutdown_event = asyncio.Event()

    async def connect_loop(self):
        url = f"{BINANCE_WS}?streams={self.stream_name}"
        logger.info(f"Connecting to {url}")

        while self.running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    logger.info("WebSocket connected successfully")
                    self.reconnect_delay = INITIAL_RECONNECT_DELAY  # Reset on success
                    await self._recv_loop(ws)
            except ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e.code} - {e.reason}")
                if not self.running:
                    break
            except WebSocketException as e:
                logger.error(f"WebSocket exception: {e}", exc_info=True)
                if not self.running:
                    break
            except asyncio.CancelledError:
                logger.info("Connection loop cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {e}", exc_info=True)
                if not self.running:
                    break

            if not self.running:
                break

            # Exponential backoff with jitter
            logger.info(f"Reconnecting in {self.reconnect_delay}s…")
            try:
                await asyncio.wait_for(
                    asyncio.sleep(self.reconnect_delay),
                    timeout=self.reconnect_delay + 1
                )
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            # Exponential backoff: double delay, cap at MAX_RECONNECT_DELAY
            self.reconnect_delay = min(self.reconnect_delay * 2, MAX_RECONNECT_DELAY)

    async def _recv_loop(self, ws):
        """Receive messages from WebSocket and batch them."""
        try:
            while self.running:
                try:
                    # Add timeout to recv to allow periodic checks
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    ts = dt.datetime.utcnow()

                    self.batch.append({"_raw": msg, "_ts": ts.isoformat()})

                    # Check batch size limits
                    batch_age = time.time() - self.batch_start
                    if (
                        batch_age >= self.conf.batch_seconds
                        or len(self.batch) >= self.conf.max_messages_per_batch
                    ):
                        await self._flush()
                    
                    # Safety check: prevent unbounded memory growth
                    if len(self.batch) > self.conf.max_messages_per_batch * 2:
                        logger.warning(
                            f"Batch size ({len(self.batch)}) exceeds safety limit, forcing flush"
                        )
                        await self._flush()

                except asyncio.TimeoutError:
                    # Timeout is expected, continue loop to check self.running
                    continue
                except ConnectionClosed:
                    logger.warning("Connection closed during receive")
                    raise
                except Exception as e:
                    logger.error(f"Error receiving message: {e}", exc_info=True)
                    raise
        except asyncio.CancelledError:
            logger.info("Receive loop cancelled")
            raise

    async def _flush(self):
        """Flush batch to S3 with proper error handling."""
        if not self.batch:
            return

        # Create a copy of the batch to upload
        batch_to_upload = self.batch.copy()
        ts = dt.datetime.utcnow()
        count = len(batch_to_upload)

        logger.info(f"Flushing {count} messages to S3…")

        try:
            key = await self.uploader.upload_messages(
                self.exchange, self.stream_name, batch_to_upload, ts
            )
            logger.info(f"Successfully uploaded → s3://{self.aws.s3_bucket_name}/{key}")
            # Only clear batch on successful upload
            self.batch = []
            self.batch_start = time.time()
        except Exception as e:
            logger.error(f"Upload failed after retries: {e}", exc_info=True)
            # Keep batch in memory for retry on next flush cycle
            # This prevents data loss but could cause memory growth if S3 is down
            logger.warning(f"Keeping {count} messages in batch for retry on next flush")
            # If batch is getting too large, we might need to write to disk as backup
            if len(self.batch) > self.conf.max_messages_per_batch * 3:
                logger.error(
                    "Batch size critical - S3 uploads failing. Consider manual intervention."
                )

    async def shutdown(self):
        """Graceful shutdown: flush pending batch and stop."""
        logger.info("Shutdown initiated, flushing pending batch...")
        self.running = False
        self._shutdown_event.set()
        
        # Flush any pending batch
        if self.batch:
            try:
                await self._flush()
            except Exception as e:
                logger.error(f"Error during final flush: {e}", exc_info=True)
        
        logger.info("Shutdown complete")

    async def run(self):
        """Main run loop with signal handling."""
        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        
        def signal_handler(sig):
            logger.info(f"Received signal {sig}, initiating graceful shutdown...")
            asyncio.create_task(self.shutdown())
        
        # Try to use add_signal_handler (Linux/Unix), fallback to signal.signal (macOS compatibility)
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
        except (NotImplementedError, RuntimeError):
            # Fallback for platforms that don't support add_signal_handler
            # (e.g., Windows, or some macOS configurations)
            logger.warning("add_signal_handler not available, using signal.signal fallback")
            def fallback_handler(sig, frame):
                signal_handler(sig)
            signal.signal(signal.SIGTERM, fallback_handler)
            signal.signal(signal.SIGINT, fallback_handler)
        
        try:
            await self.connect_loop()
        except asyncio.CancelledError:
            logger.info("Run loop cancelled")
        finally:
            await self.shutdown()
