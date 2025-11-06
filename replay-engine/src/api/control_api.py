from fastapi import FastAPI, HTTPException, Query, Depends, Response
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from typing import Optional, Dict, Any
import yaml
import redis.asyncio as redis
import asyncio
import uuid
import os
from datetime import datetime

from ..replay.deterministic_replayer import DeterministicReplayer
from ..replay.session_manager import SessionManager
from ..replay.checkpoint_store import CheckpointStore
from ..adapters.redis_stream_adapter import RedisStreamAdapter
from ..common.metrics import get_metrics
from ..common.logging_config import ReplayLogger

app = FastAPI(title="Replay Control API")
security = HTTPBearer()

# === CONFIG ===
try:
    with open("configs/replay_config.yml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
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

config["redis"]["url"] = os.getenv("REDIS_URL", config["redis"]["url"])
config["redis"]["stream_key"] = os.getenv("STREAM_KEY", config["redis"]["stream_key"])

# === INIT ===
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

# === MODELS ===
class StartRequest(BaseModel):
    session_id: Optional[str] = None
    start_ts: Optional[str] = None
    end_ts: Optional[str] = None
    mode: str = "dry-run"
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
    bugs_detected: int = 0
    elapsed_seconds: int = 0
    current_event_id: Optional[str] = None
    message: Optional[str] = None
    current_event_details: Dict[str, Any] = {}

# === AUTH ===
async def verify_token(credentials: HTTPBearer = Depends(security)):
    if credentials.credentials != "mysecret":
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials

# === STARTUP EVENT - CRITICAL FIX ===
@app.on_event("startup")
async def startup_event():
    """Connect to Redis on startup"""
    try:
        logger.info("🔌 Connecting to Redis on startup...")
        await redis_adapter.connect()
        logger.info("✅ Redis connected successfully")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")

# === ENDPOINTS ===
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
        
        # Create session (sync method)
        session_manager.create_session(replay_id, request.mode)
        
        replayer = DeterministicReplayer(redis_adapter, checkpoint_store, session_manager)
        
        async def run_replay_with_logging():
            try:
                print(f"🚀 Starting replay {replay_id}...")
                result = await replayer.execute_replay(replay_config)
                print(f"✅ Replay {replay_id} finished: {result}")
            except Exception as e:
                # FIXED: Use print() instead of logger to avoid exc_info conflict
                print(f"❌ Replay {replay_id} crashed: {e}")
                import traceback
                traceback.print_exc()
                
                # Update session on crash
                try:
                    session = session_manager.sessions.get(replay_id)
                    if session:
                        session.status = "failed"
                        session.message = str(e)
                except Exception as update_error:
                    print(f"❌ Failed to update crashed session: {update_error}")
        
        asyncio.create_task(run_replay_with_logging())
        
        logger.info(f"Replay started: {replay_id}")
        return StartResponse(replay_id=replay_id, status="started")
        
    except Exception as e:
        logger.error(f"Failed to start replay: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/replay/stop", response_model=StopResponse, dependencies=[Depends(verify_token)])
async def stop_replay(request: StopRequest):
    try:
        session = session_manager.sessions.get(request.replay_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        session.status = "stopped"
        logger.info(f"Stopped replay {request.replay_id}")
        return StopResponse(status="stopped")
    except Exception as e:
        logger.error(f"Stop failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/replay/status", response_model=StatusResponse, dependencies=[Depends(verify_token)])
async def get_status(replay_id: str = Query(...)):
    try:
        session = await session_manager.get_session(replay_id)
        if not session:
            raise HTTPException(status_code=404, detail="Not found")

        elapsed = int((datetime.now() - session.start_time).total_seconds()) if session.start_time else 0
        details = getattr(session, "current_event_details", {
            "method": "GET", "path": "Unknown", "activity": "N/A", "status": "N/A"
        })

        return StatusResponse(
            replay_id=session.replay_id,
            state=session.status,
            progress=session.progress,
            events_processed=session.events_processed,
            bugs_detected=session.bugs_detected,
            elapsed_seconds=elapsed,
            current_event_id=getattr(session, "current_event_id", None),
            message=getattr(session, "message", None),
            current_event_details=details
        )
    except Exception as e:
        logger.error(f"Status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_prometheus_metrics():
    return Response(content=get_metrics(), media_type="text/plain")