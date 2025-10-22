"""
Redis Streams Adapter for consuming events with consumer group support
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Any
import redis.asyncio as redis
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class StreamMessage:
    """Represents a message from Redis Streams"""
    stream_id: str
    fields: Dict[str, Any]
    timestamp: datetime
    
    @property
    def event_id(self) -> str:
        """Get event ID from message fields"""
        return self.fields.get("event_id", "")
    
    @property
    def session_id(self) -> Optional[str]:
        """Get session ID from message fields"""
        return self.fields.get("session_id")
    
    @property
    def request_id(self) -> Optional[str]:
        """Get request ID from message fields"""
        return self.fields.get("request_id")


class RedisStreamAdapter:
    """Redis Streams adapter with consumer group support"""
    
    def __init__(
        self,
        redis_url: str,
        stream_key: str,
        consumer_group: str,
        consumer_name: str,
        batch_size: int = 100
    ):
        self.redis_url = redis_url
        self.stream_key = stream_key
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        
        self.redis_client: Optional[redis.Redis] = None
        self.connection_pool: Optional[redis.ConnectionPool] = None
        
    async def connect(self) -> None:
        """Establish Redis connection and create consumer group"""
        try:
            self.connection_pool = redis.ConnectionPool.from_url(self.redis_url)
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            
            # Test connection
            await self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
            
            # Create consumer group if it doesn't exist
            try:
                await self.redis_client.xgroup_create(
                    self.stream_key,
                    self.consumer_group,
                    id="0",
                    mkstream=True
                )
                logger.info(f"Created consumer group '{self.consumer_group}' for stream '{self.stream_key}'")
            except redis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.info(f"Consumer group '{self.consumer_group}' already exists")
                else:
                    raise
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
        if self.connection_pool:
            await self.connection_pool.disconnect()
        logger.info("Redis connection closed")
    
    async def get_stream_info(self) -> Dict[str, Any]:
        """Get information about the Redis stream"""
        try:
            info = await self.redis_client.xinfo_stream(self.stream_key)
            return {
                "length": info.get("length", 0),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry"),
                "groups": info.get("groups", 0)
            }
        except Exception as e:
            logger.error(f"Failed to get stream info: {e}")
            return {"length": 0, "error": str(e)}
    
    async def read_new_messages(self) -> List[StreamMessage]:
        """
        Read new messages from the stream using consumer group
        
        Returns:
            List of StreamMessage objects
        """
        try:
            # Read new messages
            messages = await self.redis_client.xreadgroup(
                self.consumer_group,
                self.consumer_name,
                {self.stream_key: ">"},
                count=self.batch_size,
                block=1000  # Block for 1 second if no messages
            )
            
            result = []
            for stream_name, stream_messages in messages:
                for message_id, fields in stream_messages:
                    # Parse timestamp from message ID (Redis timestamp)
                    timestamp_ms = int(message_id.split('-')[0])
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    
                    result.append(StreamMessage(
                        stream_id=message_id,
                        fields=fields,
                        timestamp=timestamp
                    ))
            
            if result:
                logger.debug(f"Read {len(result)} new messages from stream")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to read new messages: {e}")
            return []
    
    async def read_pending_messages(self) -> List[StreamMessage]:
        """
        Read pending messages for this consumer
        
        Returns:
            List of StreamMessage objects
        """
        try:
            # Get pending messages for this consumer
            pending = await self.redis_client.xpending_range(
                self.stream_key,
                self.consumer_group,
                min="-",
                max="+",
                count=self.batch_size,
                consumer=self.consumer_name
            )
            
            if not pending:
                return []
            
            # Read the pending messages
            message_ids = [msg["message_id"] for msg in pending]
            messages = await self.redis_client.xrange(
                self.stream_key,
                min=message_ids[0],
                max=message_ids[-1]
            )
            
            result = []
            for message_id, fields in messages:
                if message_id in message_ids:
                    timestamp_ms = int(message_id.split('-')[0])
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    
                    result.append(StreamMessage(
                        stream_id=message_id,
                        fields=fields,
                        timestamp=timestamp
                    ))
            
            logger.info(f"Read {len(result)} pending messages")
            return result
            
        except Exception as e:
            logger.error(f"Failed to read pending messages: {e}")
            return []
    
    async def read_messages_by_range(
        self,
        start_id: str = "0",
        end_id: str = "+",
        count: Optional[int] = None
    ) -> List[StreamMessage]:
        """
        Read messages from a specific range (for replay)
        
        Args:
            start_id: Start message ID
            end_id: End message ID
            count: Maximum number of messages to read
            
        Returns:
            List of StreamMessage objects
        """
        try:
            messages = await self.redis_client.xrange(
                self.stream_key,
                min=start_id,
                max=end_id,
                count=count
            )
            
            result = []
            for message_id, fields in messages:
                timestamp_ms = int(message_id.split('-')[0])
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                
                result.append(StreamMessage(
                    stream_id=message_id,
                    fields=fields,
                    timestamp=timestamp
                ))
            
            logger.info(f"Read {len(result)} messages from range {start_id} to {end_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to read messages by range: {e}")
            return []
    
    async def acknowledge_message(self, message_id: str) -> bool:
        """
        Acknowledge processing of a message
        
        Args:
            message_id: Message ID to acknowledge
            
        Returns:
            True if acknowledged successfully
        """
        try:
            result = await self.redis_client.xack(
                self.stream_key,
                self.consumer_group,
                message_id
            )
            return result > 0
        except Exception as e:
            logger.error(f"Failed to acknowledge message {message_id}: {e}")
            return False
    
    async def acknowledge_messages(self, message_ids: List[str]) -> int:
        """
        Acknowledge processing of multiple messages
        
        Args:
            message_ids: List of message IDs to acknowledge
            
        Returns:
            Number of messages acknowledged
        """
        try:
            if not message_ids:
                return 0
                
            result = await self.redis_client.xack(
                self.stream_key,
                self.consumer_group,
                *message_ids
            )
            logger.debug(f"Acknowledged {result} messages")
            return result
        except Exception as e:
            logger.error(f"Failed to acknowledge messages: {e}")
            return 0
    
    async def get_consumer_info(self) -> Dict[str, Any]:
        """Get information about this consumer"""
        try:
            consumers = await self.redis_client.xinfo_consumers(
                self.stream_key,
                self.consumer_group
            )
            
            for consumer in consumers:
                if consumer["name"] == self.consumer_name:
                    return {
                        "name": consumer["name"],
                        "pending": consumer["pending"],
                        "idle": consumer["idle"]
                    }
            
            return {"name": self.consumer_name, "pending": 0, "idle": 0}
            
        except Exception as e:
            logger.error(f"Failed to get consumer info: {e}")
            return {"name": self.consumer_name, "error": str(e)}
    
    async def consume_messages(
        self,
        timeout: Optional[int] = None
    ) -> AsyncGenerator[StreamMessage, None]:
        """
        Continuously consume messages from the stream
        
        Args:
            timeout: Maximum time to wait for messages (None for infinite)
            
        Yields:
            StreamMessage objects
        """
        start_time = datetime.now()
        
        while True:
            try:
                # Check timeout
                if timeout and (datetime.now() - start_time).total_seconds() > timeout:
                    logger.info("Message consumption timeout reached")
                    break
                
                # Read new messages first
                messages = await self.read_new_messages()
                
                # If no new messages, check pending messages
                if not messages:
                    messages = await self.read_pending_messages()
                
                # Yield messages
                for message in messages:
                    yield message
                
                # If no messages, sleep briefly
                if not messages:
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in message consumption: {e}")
                await asyncio.sleep(1)  # Wait before retrying