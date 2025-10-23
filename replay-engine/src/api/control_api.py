from fastapi import FastAPI, HTTPException, Query, Depends, Response # type: ignore
from fastapi.security import HTTPBearer # type: ignore
from pydantic import BaseModel
from typing import Optional
import yaml
import redis.asyncio as redis # type: ignore
import asyncio
import uuid
import os
from ..replay.deterministic_replayer import DeterministicReplayer
from ..replay.session_manager import SessionManager
from ..replay.checkpoint_store import CheckpointStore
from ..adapters.redis_stream_adapter import RedisStreamAdapter
from ..common.metrics import get_metrics
from ..common.logging_config import ReplayLogger

# FastAPI app setup
app = FastAPI(title="Replay Control API")
security = HTTPBearer()

# Load configuration
try:
    with open("configs/replay_config.yml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    # Fallback config if file not found
    config = {
        "redis": {
            "url": os.getenv("REDIS_URL", "redis://localhost:6379"),
            "stream_key": os.getenv("STREAM_KEY", "logs:stream"),
            "consumer_group": "replay_group",
            "consumer_name": "replayer-1"
        },
        "replay": {
            "checkpoint_every": 10,
            "speed": 1.0
        }
    }

# Override with environment variables if present
config["redis"]["url"] = os.getenv("REDIS_URL", config["redis"]["url"])
config["redis"]["stream_key"] = os.getenv("STREAM_KEY", config["redis"]["stream_key"])

# Initialize components
redis_client = redis.Redis.from_url(config["redis"]["url"])
redis_adapter = RedisStreamAdapter(
    redis_url=config["redis"]["url"],
    stream_key=config["redis"]["stream_key"],
    consumer_group=config["redis"]["consumer_group"],
    consumer_name=config["redis"]["consumer_name"]
)
checkpoint_store = CheckpointStore(redis_client)
session_manager = SessionManager()
logger = ReplayLogger(__name__)

# Pydantic models
class StartRequest(BaseModel):
    session_id: Optional[str] = None
    start_ts: Optional[str] = None
    end_ts: Optional[str] = None
    mode: str = "dry-run"  # dry-run|live|timed
    speed: float = 1.0

class StartResponse(BaseModel):
    replay_id: str
    status: str

class StopRequest(BaseModel):
    replay_id: str

class StopResponse(BaseModel):
    status: str

class StatusResponse(BaseModel):
    replay_id: str
    state: str
    progress: float
    events_processed: int

# Dependency for token verification
async def verify_token(credentials: HTTPBearer = Depends(security)):
    token = credentials.credentials
    if token != "mysecret":
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials

# Endpoints
@app.get("/health")
async def health():
    try:
        await redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}")

@app.post("/replay/start", response_model=StartResponse, dependencies=[Depends(verify_token)])
async def start_replay(request: StartRequest):
    try:
        replay_id = f"r-{uuid.uuid4().hex[:8]}"
        
        # Create replay config
        replay_config = {
            "replay_id": replay_id,
            "session_id": request.session_id,
            "start_ts": request.start_ts,
            "end_ts": request.end_ts,
            "mode": request.mode,
            "speed": request.speed,
            "checkpoint_every": config["replay"]["checkpoint_every"]
        }
        
        # Create session with config
        await session_manager.create_session(replay_id, replay_config)
        
        # Create replayer
        replayer = DeterministicReplayer(redis_adapter, checkpoint_store, session_manager)
        
        # Background task with exception handling
        async def run_replay_with_logging():
            try:
                result = await replayer.execute_replay(replay_config)
                logger.info(f"Replay {replay_id} finished: {result}")
            except Exception as e:
                logger.error(f"Replay {replay_id} crashed: {e}", exc_info=True)
                await session_manager.update_session_status(replay_id, "failed")
        
        asyncio.create_task(run_replay_with_logging())
        
        logger.info(f"Started replay {replay_id}")
        return StartResponse(replay_id=replay_id, status="started")
    except Exception as e:
        logger.error(f"Failed to start replay: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/replay/stop", response_model=StopResponse, dependencies=[Depends(verify_token)])
async def stop_replay(request: StopRequest):
    try:
        success = await session_manager.update_session_status(request.replay_id, "stopped")
        if not success:
            raise HTTPException(status_code=404, detail="Replay session not found")
        logger.info(f"Stopped replay {request.replay_id}")
        return StopResponse(status="stopped")
    except Exception as e:
        logger.error(f"Failed to stop replay: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/replay/status", response_model=StatusResponse, dependencies=[Depends(verify_token)])
async def get_status(replay_id: str = Query(...)):
    try:
        session = await session_manager.get_session(replay_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Replay session '{replay_id}' not found")
        return StatusResponse(
            replay_id=session.replay_id,
            state=session.status,
            progress=session.progress,
            events_processed=session.events_processed
        )
    except Exception as e:
        logger.error(f"Failed to get status for {replay_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")

@app.get("/metrics")
async def get_prometheus_metrics():
    return Response(content=get_metrics(), media_type="text/plain")