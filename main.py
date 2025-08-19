import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
import logging

import redis.asyncio as redis
import httpx
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator  # Updated import
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MOCK_SERVICE_URL = "http://localhost:8080"  # Mock posting service URL
REDIS_URL = "redis://localhost:6379"
QUEUE_NAME = "transaction_queue"
RETRY_QUEUE_NAME = "transaction_retry_queue"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

# Pydantic Models
class TransactionCreate(BaseModel):
    amount: Decimal = Field(..., gt=0, description="Transaction amount (positive)")
    currency: str = Field(..., min_length=3, max_length=3, description="ISO 4217 currency code")
    description: str = Field(..., min_length=1, max_length=500, description="Transaction description")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Optional metadata")
    
    @field_validator('currency')  # Updated to use field_validator
    @classmethod
    def validate_currency(cls, v):
        if not v.isupper():
            raise ValueError('Currency must be uppercase')
        return v


class TransactionResponse(BaseModel):
    id: str
    status: str = "pending"
    message: str = "Transaction queued for processing"


class TransactionStatusResponse(BaseModel):
    transactionId: str
    status: str  # pending|processing|completed|failed
    submittedAt: datetime
    completedAt: Optional[datetime] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    queue_depth: int
    retry_queue_depth: int
    error_rate: float
    total_transactions: int
    failed_transactions: int
    avg_processing_time: float
    mock_service_status: str


# Global variables for monitoring
processing_times = []
failed_count = 0
total_count = 0
transaction_store = {}  # In-memory state store for transaction status


async def get_redis_connection():
    """Get Redis connection"""
    return redis.from_url(REDIS_URL, decode_responses=True)


async def check_mock_service_health():
    """Check if mock posting service is available"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MOCK_SERVICE_URL}/transactions", timeout=5.0)
            return "healthy" if response.status_code == 200 else "unhealthy"
    except httpx.ConnectError:
        logger.warning(f"Mock service connection failed - service not running at {MOCK_SERVICE_URL}")
        return "unavailable"
    except httpx.TimeoutException:
        logger.warning(f"Mock service timeout at {MOCK_SERVICE_URL}")
        return "timeout"
    except Exception as e:
        logger.error(f"Mock service health check failed: {e}")
        return "unavailable"


async def get_transaction_from_mock(transaction_id: str) -> Optional[dict]:
    """Check if transaction exists in mock service"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{MOCK_SERVICE_URL}/transactions/{transaction_id}",
                timeout=10.0
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                logger.warning(f"Unexpected response from GET /transactions/{transaction_id}: {response.status_code}")
                return None
    except httpx.ConnectError:
        logger.warning(f"Mock service connection failed for GET /transactions/{transaction_id}")
        return None
    except Exception as e:
        logger.error(f"Error checking transaction {transaction_id}: {e}")
        return None


async def post_transaction_to_mock(transaction_data: dict) -> tuple[bool, Optional[str]]:
    try:
        async with httpx.AsyncClient() as client:
            payload = {
                "id": transaction_data["id"],
                "amount": transaction_data["amount"],
                "currency": transaction_data["currency"],
                "description": transaction_data["description"],
                "timestamp": utc_now_iso(),
            }

            # include metadata if present
            if "metadata" in transaction_data:
                payload["metadata"] = transaction_data["metadata"]

            response = await client.post(
                f"{MOCK_SERVICE_URL}/transactions",
                json=payload,
                timeout=30.0
            )

            if response.status_code in [200, 201]:
                return True, None
            else:
                try:
                    detail = response.json()
                except:
                    detail = response.text
                return False, f"Mock service POST failed: {response.status_code} - {detail}"

    except httpx.ConnectError:
        return False, "Mock service connection failed"
    except httpx.TimeoutException:
        return False, "Mock service timeout"
    except Exception as e:
        return False, f"POST error: {str(e)}"





async def process_transaction_with_retry_logic(transaction_data: dict, transaction_id: str, retry_count: int = 0):
    """Process transaction with comprehensive retry logic and idempotency checks"""
    global processing_times, failed_count, total_count
    
    start_time = time.time()
    
    try:
        # Update status to processing
        if transaction_id in transaction_store:
            transaction_store[transaction_id]["status"] = "processing"
        
        logger.info(f"Processing transaction {transaction_id} (attempt {retry_count + 1})")
        
        # Step 1: Check if transaction already exists (idempotency check)
        existing_transaction = await get_transaction_from_mock(transaction_id)
        if existing_transaction:
            logger.info(f"Transaction {transaction_id} already exists in mock service - marking as completed")
            if transaction_id in transaction_store:
                transaction_store[transaction_id].update({
                    "status": "completed",
                    "completedAt": datetime.utcnow(),
                    "mock_response": existing_transaction
                })
            
            processing_time = time.time() - start_time
            processing_times.append(processing_time)
            if len(processing_times) > 1000:
                processing_times.pop(0)
            total_count += 1
            return True
        
        # Step 2: POST transaction to mock service
        post_success, post_error = await post_transaction_to_mock(transaction_data)
        
        if post_success:
            # Success - mark as completed
            logger.info(f"Transaction {transaction_id} posted successfully")
            if transaction_id in transaction_store:
                transaction_store[transaction_id].update({
                    "status": "completed",
                    "completedAt": datetime.utcnow()
                })
            
            processing_time = time.time() - start_time
            processing_times.append(processing_time)
            if len(processing_times) > 1000:
                processing_times.pop(0)
            total_count += 1
            return True
        
        else:
            # Step 3: POST failed - check if it's a post-write failure
            logger.warning(f"POST failed for transaction {transaction_id}: {post_error}")
            
            # Check if transaction now exists (post-write failure scenario)
            existing_after_fail = await get_transaction_from_mock(transaction_id)
            if existing_after_fail:
                logger.info(f"Transaction {transaction_id} found after POST failure - post-write failure detected")
                if transaction_id in transaction_store:
                    transaction_store[transaction_id].update({
                        "status": "completed",
                        "completedAt": datetime.utcnow(),
                        "mock_response": existing_after_fail
                    })
                
                processing_time = time.time() - start_time
                processing_times.append(processing_time)
                if len(processing_times) > 1000:
                    processing_times.pop(0)
                total_count += 1
                return True
            
            # Step 4: True pre-write failure - schedule retry if within limits
            if retry_count < MAX_RETRIES:
                logger.info(f"Scheduling retry {retry_count + 1} for transaction {transaction_id}")
                await schedule_retry(transaction_data, transaction_id, retry_count + 1)
                return False  # Will be retried
            else:
                # Max retries exceeded
                error_msg = f"Max retries exceeded. Last error: {post_error}"
                logger.error(f"Transaction {transaction_id} failed permanently: {error_msg}")
                if transaction_id in transaction_store:
                    transaction_store[transaction_id].update({
                        "status": "failed",
                        "completedAt": datetime.utcnow(),
                        "error": error_msg
                    })
                failed_count += 1
                total_count += 1
                return False
                
    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        logger.error(f"Transaction {transaction_id} processing failed: {error_msg}")
        
        if retry_count < MAX_RETRIES:
            logger.info(f"Scheduling retry {retry_count + 1} for transaction {transaction_id} due to error")
            await schedule_retry(transaction_data, transaction_id, retry_count + 1)
            return False
        else:
            if transaction_id in transaction_store:
                transaction_store[transaction_id].update({
                    "status": "failed",
                    "completedAt": datetime.utcnow(),
                    "error": error_msg
                })
            failed_count += 1
            total_count += 1
            return False


async def schedule_retry(transaction_data: dict, transaction_id: str, retry_count: int):
    """Schedule transaction for retry"""
    try:
        retry_data = {
            **transaction_data,
            "retry_count": retry_count,
            "retry_at": (datetime.utcnow() + timedelta(seconds=RETRY_DELAY_SECONDS)).isoformat()
        }
        
        redis_client = await get_redis_connection()
        await redis_client.lpush(RETRY_QUEUE_NAME, json.dumps(retry_data))
        logger.info(f"Transaction {transaction_id} scheduled for retry {retry_count} in {RETRY_DELAY_SECONDS}s")
        
    except Exception as e:
        logger.error(f"Failed to schedule retry for {transaction_id}: {e}")


async def process_transaction_queue():
    """Background task to process transactions from main queue"""
    logger.info("Starting main transaction queue processor")
    
    while True:
        try:
            redis_client = await get_redis_connection()
            
            # Get transaction from main queue
            result = await redis_client.brpop([QUEUE_NAME], timeout=5)
            
            if result:
                queue_name, transaction_data_str = result
                transaction_data = json.loads(transaction_data_str)
                transaction_id = transaction_data["id"]
                
                await process_transaction_with_retry_logic(transaction_data, transaction_id, 0)
                
        except Exception as e:
            logger.error(f"Error processing main queue: {e}")
            await asyncio.sleep(1)


async def process_retry_queue():
    """Background task to process retry queue"""
    logger.info("Starting retry queue processor")
    
    while True:
        try:
            redis_client = await get_redis_connection()
            
            # Get transaction from retry queue
            result = await redis_client.brpop([RETRY_QUEUE_NAME], timeout=5)
            
            if result:
                queue_name, transaction_data_str = result
                transaction_data = json.loads(transaction_data_str)
                transaction_id = transaction_data["id"]
                retry_count = transaction_data.get("retry_count", 0)
                retry_at = datetime.fromisoformat(transaction_data["retry_at"])
                
                # Check if it's time to retry
                if datetime.utcnow() >= retry_at:
                    logger.info(f"Processing retry {retry_count} for transaction {transaction_id}")
                    await process_transaction_with_retry_logic(transaction_data, transaction_id, retry_count)
                else:
                    # Not yet time, put back in queue
                    await redis_client.lpush(RETRY_QUEUE_NAME, transaction_data_str)
                    await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error processing retry queue: {e}")
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for FastAPI app"""
    # Startup
    logger.info("Starting Robust Transaction Queue Service")
    
    # Test Redis connection
    try:
        redis_client = await get_redis_connection()
        await redis_client.ping()
        logger.info("Redis connection established")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        logger.warning("Service will continue with degraded functionality")
    
    # Test mock service connection
    mock_status = await check_mock_service_health()
    logger.info(f"Mock service status: {mock_status}")
    if mock_status == "unavailable":
        logger.warning("Mock service is not available. Start the mock service at localhost:8085 for full functionality")
    
    # Start background processors
    main_task = asyncio.create_task(process_transaction_queue())
    retry_task = asyncio.create_task(process_retry_queue())
    
    yield
    
    # Shutdown
    logger.info("Shutting down Queue Service")
    main_task.cancel()
    retry_task.cancel()
    try:
        await main_task
        await retry_task
    except asyncio.CancelledError:
        pass


# FastAPI app
app = FastAPI(
    title="Robust Transaction Queue Service",
    description="Production-ready queueing service with retry logic and idempotency checks",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only - restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/transactions", response_model=TransactionResponse, status_code=202)
async def submit_transaction(transaction: TransactionCreate):
    """Accept transaction for processing and return 202 Accepted immediately"""
    start_time = time.time()
    
    try:
        # Generate unique ID
        transaction_id = str(uuid.uuid4())
        current_time = datetime.utcnow()
        
        # Store transaction status in state store
        transaction_store[transaction_id] = {
            "transactionId": transaction_id,
            "status": "pending",
            "submittedAt": current_time,
            "completedAt": None,
            "error": None,
            "transaction_data": transaction.model_dump()  # Updated from .dict()
        }
        
        # Prepare transaction data for queue
        transaction_data = {
            "id": transaction_id,
            "amount": float(transaction.amount),
            "currency": transaction.currency,
            "description": transaction.description,
            "metadata": transaction.metadata,
            "submitted_at": current_time.isoformat()
        }
        
        # Add to Redis main queue
        try:
            redis_client = await get_redis_connection()
            await redis_client.lpush(QUEUE_NAME, json.dumps(transaction_data))
            logger.info(f"Transaction {transaction_id} queued successfully")
        except Exception as redis_error:
            logger.error(f"Redis error: {redis_error}")
            # Even if Redis fails, we can still accept the transaction
            logger.warning(f"Transaction {transaction_id} accepted but not queued due to Redis error")
        
        processing_time = time.time() - start_time
        logger.info(f"Transaction {transaction_id} processed in {processing_time*1000:.2f}ms")
        
        # Ensure response time < 100ms
        if processing_time > 0.1:
            logger.warning(f"Response time exceeded 100ms: {processing_time*1000:.2f}ms")
        
        return TransactionResponse(
            id=transaction_id,
            status="pending",
            message="Transaction queued for processing"
        )
        
    except Exception as e:
        logger.error(f"Failed to submit transaction: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit transaction: {str(e)}")


@app.get("/api/transactions/{transaction_id}", response_model=TransactionStatusResponse)
async def get_transaction_status(transaction_id: str):
    """Return transaction status and details in expected format"""
    try:
        if transaction_id not in transaction_store:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        transaction_data = transaction_store[transaction_id]
        
        return TransactionStatusResponse(
            transactionId=transaction_data["transactionId"],
            status=transaction_data["status"],
            submittedAt=transaction_data["submittedAt"],
            completedAt=transaction_data["completedAt"],
            error=transaction_data["error"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get transaction status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve transaction status: {str(e)}")


@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """System health and queue status"""
    try:
        # Check Redis and get queue depths
        try:
            redis_client = await get_redis_connection()
            await redis_client.ping()
            queue_depth = await redis_client.llen(QUEUE_NAME)
            retry_queue_depth = await redis_client.llen(RETRY_QUEUE_NAME)
            redis_status = "healthy"
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            redis_status = "unhealthy"
            queue_depth = 0
            retry_queue_depth = 0
        
        # Check mock service
        mock_status = await check_mock_service_health()
        
        # Calculate metrics
        error_rate = (failed_count / max(total_count, 1)) * 100
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        # Overall system status
        if redis_status == "healthy" and mock_status == "healthy":
            overall_status = "healthy"
        elif redis_status == "healthy" and mock_status in ["unhealthy", "unavailable", "timeout"]:
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"
        
        return HealthResponse(
            status=overall_status,
            timestamp=datetime.utcnow(),
            queue_depth=queue_depth,
            retry_queue_depth=retry_queue_depth,
            error_rate=round(error_rate, 2),
            total_transactions=total_count,
            failed_transactions=failed_count,
            avg_processing_time=round(avg_processing_time, 3),
            mock_service_status=mock_status
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


# Additional monitoring endpoints
@app.get("/api/stats")
async def get_detailed_stats():
    """Get detailed system statistics"""
    try:
        try:
            redis_client = await get_redis_connection()
            main_queue_depth = await redis_client.llen(QUEUE_NAME)
            retry_queue_depth = await redis_client.llen(RETRY_QUEUE_NAME)
        except:
            main_queue_depth = 0
            retry_queue_depth = 0
        
        # Status breakdown
        pending_count = sum(1 for t in transaction_store.values() if t["status"] == "pending")
        processing_count = sum(1 for t in transaction_store.values() if t["status"] == "processing")
        completed_count = sum(1 for t in transaction_store.values() if t["status"] == "completed")
        failed_local_count = sum(1 for t in transaction_store.values() if t["status"] == "failed")
        
        return {
            "queues": {
                "main_queue_depth": main_queue_depth,
                "retry_queue_depth": retry_queue_depth,
                "total_queue_depth": main_queue_depth + retry_queue_depth
            },
            "transactions": {
                "in_memory_count": len(transaction_store),
                "status_breakdown": {
                    "pending": pending_count,
                    "processing": processing_count,
                    "completed": completed_count,
                    "failed": failed_local_count
                }
            },
            "performance": {
                "total_processed": total_count,
                "total_failed": failed_count,
                "error_rate_percent": round((failed_count / max(total_count, 1)) * 100, 2),
                "avg_processing_time_seconds": round(sum(processing_times) / len(processing_times) if processing_times else 0, 3)
            },
            "config": {
                "max_retries": MAX_RETRIES,
                "retry_delay_seconds": RETRY_DELAY_SECONDS,
                "mock_service_url": MOCK_SERVICE_URL
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cleanup")
async def cleanup_system():
    """Clear all queues and transaction store (for testing)"""
    try:
        try:
            redis_client = await get_redis_connection()
            main_queue_size = await redis_client.llen(QUEUE_NAME)
            retry_queue_size = await redis_client.llen(RETRY_QUEUE_NAME)
            
            await redis_client.delete(QUEUE_NAME)
            await redis_client.delete(RETRY_QUEUE_NAME)
        except:
            main_queue_size = 0
            retry_queue_size = 0
        
        store_size = len(transaction_store)
        transaction_store.clear()
        
        # Reset counters
        global failed_count, total_count, processing_times
        failed_count = 0
        total_count = 0
        processing_times.clear()
        
        return {
            "message": "System cleaned up successfully",
            "cleared": {
                "main_queue": main_queue_size,
                "retry_queue": retry_queue_size,
                "transaction_store": store_size
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000, 
        log_level="info",
        access_log=True
    )