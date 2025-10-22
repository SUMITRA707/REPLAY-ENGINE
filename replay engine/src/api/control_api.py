import asyncio
import datetime

from flask import Response
from fastapi import FastAPI, HTTPException, Query, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, Dict, Any
import yaml
import redis.asyncio as redis
from ..replay.deterministic_replayer import DeterministicReplayer
from ..replay.session_manager import SessionManager, ReplaySession
from ..replay.checkpoint_store import CheckpointStore
from ..adapters.redis_stream_adapter import RedisStreamAdapter
from ..common.metrics import MetricsCollector, get_metrics
from ..common.logging_config import ReplayLogger
import logging

app = FastAPI(title="Replay Control API")
security = HTTPBearer()

# Load configuration
with open("configs/replay_config.yml", "r") as f:
    config = yaml.safe_load(f)

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

async def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if not config["security"]["enable_auth"]:
        return
    if credentials.credentials != config["security"]["shared_token"]:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

@app.on_event("startup")
async def startup_event():
    await redis_adapter.connect()
    logger.info("Control API started")

@app.on_event("shutdown")
async def shutdown_event():
    await redis_adapter.disconnect()
    await redis_client.close()
    logger.info("Control API shutdown")

@app.post("/replay/start", response_model=StartResponse, dependencies=[Depends(verify_token)])
async def start_replay(request: StartRequest):
    try:
        replay_id = f"replay-{int(datetime.now().timestamp())}"
        session = await session_manager.create_session(replay_id, request.dict())
        logger.set_replay_id(replay_id)
        
        replayer = DeterministicReplayer(redis_adapter, checkpoint_store, session_manager)
        asyncio.create_task(replayer.execute_replay({
            "replay_id": replay_id,
            "session_id": request.session_id,
            "start_ts": request.start_ts,
            "end_ts": request.end_ts,
            "mode": request.mode,
            "speed": request.speed,
            "checkpoint_every": config["replay"]["checkpoint_every"]
        }))
        
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
            raise HTTPException(status_code=404, detail="Replay session not found")
        return StatusResponse(
            replay_id=session.replay_id,
            state=session.status,
            progress=session.progress,
            events_processed=session.events_processed
        )
    except Exception as e:
        logger.error(f"Failed to get status for {replay_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_prometheus_metrics():
    return Response(content=get_metrics(), media_type="text/plain")