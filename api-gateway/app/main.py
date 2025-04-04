# api-gateway/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import uuid
import os
import httpx
from datetime import datetime
import structlog
import json

from kafka_admin import KafkaAdmin
from kafka_producer import KafkaProducer

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()

# Initialize FastAPI app
app = FastAPI(
    title="API Gateway",
    description="FastAPI service that serves as an API Gateway and provides Kafka monitoring",
    version="1.0.0",
)

# Define Kafka topics
ORDERS_TOPIC = "orders"
NOTIFICATIONS_TOPIC = "notifications"
ANALYTICS_TOPIC = "analytics"


# Pydantic models for request validation
class OrderItem(BaseModel):
    product_id: str
    quantity: int
    price: float


class OrderRequest(BaseModel):
    customer_id: str
    items: List[OrderItem]
    shipping_address: Dict[str, str]
    payment_method: str
    order_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))


class NotificationRequest(BaseModel):
    recipient_id: str
    notification_type: str
    content: str
    priority: str = "normal"
    notification_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))


class AnalyticsEvent(BaseModel):
    event_type: str
    user_id: Optional[str] = None
    properties: Dict[str, Any]
    timestamp: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())
    event_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))


class TopicRequest(BaseModel):
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = {}


# Dependency for Kafka admin
def get_kafka_admin():
    admin = KafkaAdmin(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    return admin


# Dependency for Kafka producer
def get_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    try:
        yield producer
    finally:
        producer.flush()


# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "api-gateway"}


# Kafka topics management endpoints
@app.get("/api/kafka/topics")
def list_topics(admin: KafkaAdmin = Depends(get_kafka_admin)):
    try:
        topics = admin.list_topics()
        return {"topics": topics}
    except Exception as e:
        logger.error("Failed to list topics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to list Kafka topics")


@app.post("/api/kafka/topics", status_code=201)
def create_topic(topic: TopicRequest, admin: KafkaAdmin = Depends(get_kafka_admin)):
    try:
        result = admin.create_topics([topic.dict()])
        if result.get(topic.name, False):
            return {"message": f"Topic {topic.name} created successfully"}
        else:
            raise HTTPException(
                status_code=500, detail=f"Failed to create topic {topic.name}"
            )
    except Exception as e:
        logger.error("Failed to create topic", error=str(e), topic=topic.name)
        raise HTTPException(status_code=500, detail=f"Failed to create topic: {str(e)}")


@app.delete("/api/kafka/topics/{topic_name}")
def delete_topic(topic_name: str, admin: KafkaAdmin = Depends(get_kafka_admin)):
    try:
        result = admin.delete_topics([topic_name])
        if result.get(topic_name, False):
            return {"message": f"Topic {topic_name} deleted successfully"}
        else:
            raise HTTPException(
                status_code=500, detail=f"Failed to delete topic {topic_name}"
            )
    except Exception as e:
        logger.error("Failed to delete topic", error=str(e), topic=topic_name)
        raise HTTPException(status_code=500, detail=f"Failed to delete topic: {str(e)}")


# Order API endpoints (proxy to producer service)
@app.post("/api/orders", status_code=201)
async def create_order(order: OrderRequest):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://producer-service:8000/api/orders", json=order.dict()
            )
            return response.json()
    except Exception as e:
        logger.error("Failed to forward order request", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to process order")


# Notification API endpoints (proxy to producer service)
@app.post("/api/notifications", status_code=201)
async def send_notification(notification: NotificationRequest):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://producer-service:8000/api/notifications",
                json=notification.dict(),
            )
            return response.json()
    except Exception as e:
        logger.error("Failed to forward notification request", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to send notification")


# Analytics API endpoints (proxy to producer service)
@app.post("/api/analytics/events", status_code=201)
async def track_event(event: AnalyticsEvent):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://producer-service:8000/api/analytics/events", json=event.dict()
            )
            return response.json()
    except Exception as e:
        logger.error("Failed to forward analytics event", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to track analytics event")


@app.post("/api/analytics/bulk-events", status_code=201)
async def track_bulk_events(events: List[AnalyticsEvent]):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://producer-service:8000/api/analytics/bulk-events",
                json=[event.dict() for event in events],
            )
            return response.json()
    except Exception as e:
        logger.error("Failed to forward bulk analytics events", error=str(e))
        raise HTTPException(
            status_code=500, detail="Failed to track bulk analytics events"
        )


# Direct message production endpoint (for testing)
@app.post("/api/kafka/produce/{topic}")
async def produce_message(
    topic: str, request: Request, producer: KafkaProducer = Depends(get_kafka_producer)
):
    try:
        # Get raw request body
        body = await request.json()

        # Extract key if provided
        key = body.pop("key", None) if isinstance(body, dict) else None

        # Produce message to Kafka
        producer.produce(topic=topic, value=body, key=key)
        producer.flush()

        return {
            "status": "success",
            "message": f"Message produced to topic {topic}",
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error("Failed to produce message", error=str(e), topic=topic)
        raise HTTPException(
            status_code=500, detail=f"Failed to produce message: {str(e)}"
        )


# Middleware to log requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = datetime.now()

    # Process the request
    response = await call_next(request)

    # Calculate processing time
    process_time = (datetime.now() - start_time).total_seconds() * 1000

    # Log request details
    logger.info(
        "Request processed",
        method=request.method,
        url=str(request.url),
        status_code=response.status_code,
        process_time_ms=process_time,
    )

    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
