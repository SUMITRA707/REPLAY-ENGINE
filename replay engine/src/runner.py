import asyncio
import argparse
import yaml
from datetime import datetime
import redis.asyncio as redis
from replay.deterministic_replayer import DeterministicReplayer
from adapters.redis_stream_adapter import RedisStreamAdapter
from replay.checkpoint_store import CheckpointStore
from replay.session_manager import SessionManager
from common.logging_config import ReplayLogger
import logging

async def main():
    parser = argparse.ArgumentParser(description="Replay Engine CLI")
    parser.add_argument("--session-id", type=str, help="Session ID for replay")
    parser.add_argument("--mode", default="dry-run", choices=["dry-run", "live", "timed"], help="Replay mode")
    parser.add_argument("--speed", type=float, default=1.0, help="Replay speed multiplier")
    parser.add_argument("--config", default="configs/replay_config.yml", help="Config file path")
    parser.add_argument("--start-ts", type=str, help="Start timestamp (ISO8601)")
    parser.add_argument("--end-ts", type=str, help="End timestamp (ISO8601)")
    args = parser.parse_args()

    logger = ReplayLogger(__name__)

    try:
        # Load configuration
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        # Initialize components
        redis_client = redis.Redis.from_url(config["redis"]["url"])
        redis_adapter = RedisStreamAdapter(
            redis_url=config["redis"]["url"],
            stream_key=config["redis"]["stream_key"],
            consumer_group=config["redis"]["consumer_group"],
            consumer_name=config["redis"]["consumer_name"]
        )
        await redis_adapter.connect()
        checkpoint_store = CheckpointStore(redis_client)
        session_manager = SessionManager()

        # Create replayer
        replayer = DeterministicReplayer(redis_adapter, checkpoint_store, session_manager)

        # Execute replay
        replay_id = f"cli-replay-{int(datetime.now().timestamp())}"
        result = await replayer.execute_replay({
            "replay_id": replay_id,
            "session_id": args.session_id,
            "start_ts": args.start_ts,
            "end_ts": args.end_ts,
            "mode": args.mode,
            "speed": args.speed,
            "checkpoint_every": config["replay"]["checkpoint_every"]
        })

        logger.info(f"Replay completed: {result}")
        print(result)

    except Exception as e:
        logger.error(f"CLI failed: {str(e)}")
        print(f"Error: {str(e)}")
        raise

    finally:
        await redis_adapter.disconnect()
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(main())