"""
Redis-backed checkpoint store for replay state persistence
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import redis.asyncio as redis # type: ignore

from ..common.logging_config import ReplayLogger

logger = ReplayLogger(__name__)

class CheckpointStore:
    """Redis-backed checkpoint store for replay state persistence"""
    
    def __init__(self, redis_client: redis.Redis, prefix: str = "replay:checkpoint"):
        self.redis_client = redis_client
        self.prefix = prefix
    
    def _get_key(self, replay_id: str, checkpoint_type: str = "main") -> str:
        """Generate Redis key for checkpoint"""
        return f"{self.prefix}:{replay_id}:{checkpoint_type}"
    
    async def save_checkpoint(
        self,
        replay_id: str,
        checkpoint_data: Dict[str, Any],
        checkpoint_type: str = "main"
    ) -> bool:
        """
        Save checkpoint data to Redis
        
        Args:
            replay_id: Unique replay session identifier
            checkpoint_data: Checkpoint data to save
            checkpoint_type: Type of checkpoint (main, progress, etc.)
            
        Returns:
            True if saved successfully
        """
        try:
            key = self._get_key(replay_id, checkpoint_type)
            
            # Add metadata
            checkpoint_data["saved_at"] = datetime.utcnow().isoformat() + "Z"
            checkpoint_data["replay_id"] = replay_id
            checkpoint_data["checkpoint_type"] = checkpoint_type
            
            # Store as JSON in Redis hash
            await self.redis_client.hset(key, mapping={
                "data": json.dumps(checkpoint_data),
                "timestamp": checkpoint_data["saved_at"]
            })
            
            # Set expiration (24 hours)
            await self.redis_client.expire(key, 86400)
            
            logger.debug(f"Saved checkpoint for replay {replay_id} ({checkpoint_type})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save checkpoint for {replay_id}: {e}")
            return False
    
    async def load_checkpoint(
        self,
        replay_id: str,
        checkpoint_type: str = "main"
    ) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint data from Redis
        
        Args:
            replay_id: Unique replay session identifier
            checkpoint_type: Type of checkpoint to load
            
        Returns:
            Checkpoint data or None if not found
        """
        try:
            key = self._get_key(replay_id, checkpoint_type)
            
            # Get checkpoint data
            checkpoint_hash = await self.redis_client.hgetall(key)
            
            if not checkpoint_hash:
                logger.debug(f"No checkpoint found for replay {replay_id} ({checkpoint_type})")
                return None
            
            # Parse JSON data
            checkpoint_data = json.loads(checkpoint_hash[b"data"].decode())
            
            logger.debug(f"Loaded checkpoint for replay {replay_id} ({checkpoint_type})")
            return checkpoint_data
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint for {replay_id}: {e}")
            return None
    
    async def delete_checkpoint(
        self,
        replay_id: str,
        checkpoint_type: str = "main"
    ) -> bool:
        """
        Delete checkpoint data from Redis
        
        Args:
            replay_id: Unique replay session identifier
            checkpoint_type: Type of checkpoint to delete
            
        Returns:
            True if deleted successfully
        """
        try:
            key = self._get_key(replay_id, checkpoint_type)
            result = await self.redis_client.delete(key)
            
            if result > 0:
                logger.debug(f"Deleted checkpoint for replay {replay_id} ({checkpoint_type})")
                return True
            else:
                logger.debug(f"No checkpoint found to delete for replay {replay_id} ({checkpoint_type})")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete checkpoint for {replay_id}: {e}")
            return False
    
    async def save_progress_checkpoint(
        self,
        replay_id: str,
        progress_data: Dict[str, Any]
    ) -> bool:
        """
        Save progress-specific checkpoint data
        
        Args:
            replay_id: Unique replay session identifier
            progress_data: Progress data to save
            
        Returns:
            True if saved successfully
        """
        return await self.save_checkpoint(replay_id, progress_data, checkpoint_type="progress")
    
    async def load_progress_checkpoint(
        self,
        replay_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Load progress-specific checkpoint data
        
        Args:
            replay_id: Unique replay session identifier
            
        Returns:
            Progress checkpoint data or None if not found
        """
        return await self.load_checkpoint(replay_id, checkpoint_type="progress")
    
    async def list_checkpoints(
        self,
        replay_id: str
    ) -> List[str]:
        """
        List all checkpoint types for a replay
        
        Args:
            replay_id: Unique replay session identifier
            
        Returns:
            List of checkpoint types
        """
        try:
            pattern = f"{self.prefix}:{replay_id}:*"
            keys = await self.redis_client.keys(pattern)
            checkpoint_types = [
                key.decode().split(":")[-1] for key in keys
            ]
            logger.debug(f"Found {len(checkpoint_types)} checkpoints for replay {replay_id}")
            return checkpoint_types
            
        except Exception as e:
            logger.error(f"Failed to list checkpoints for {replay_id}: {e}")
            return []
    
    async def clear_all_checkpoints(self, replay_id: str) -> bool:
        """
        Clear all checkpoints for a replay
        
        Args:
            replay_id: Unique replay session identifier
            
        Returns:
            True if cleared successfully
        """
        try:
            checkpoint_types = await self.list_checkpoints(replay_id)
            if not checkpoint_types:
                logger.debug(f"No checkpoints to clear for replay {replay_id}")
                return True
                
            deleted = 0
            for checkpoint_type in checkpoint_types:
                if await self.delete_checkpoint(replay_id, checkpoint_type):
                    deleted += 1
            
            logger.debug(f"Cleared {deleted} checkpoints for replay {replay_id}")
            return deleted == len(checkpoint_types)
            
        except Exception as e:
            logger.error(f"Failed to clear checkpoints for {replay_id}: {e}")
            return False