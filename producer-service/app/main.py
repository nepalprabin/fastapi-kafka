# producer-service/app/main.py
from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import uuid
import os
from datetime import datetime
import structlog

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
    title="Producer Service",
    description="FastAPI service that produces messages to Kafka",
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
    return {"status": "healthy", "service": "producer-service"}


# Order processing endpoint
@app.post("/api/orders", status_code=201)
async def create_order(
    order: OrderRequest,
    background_tasks: BackgroundTasks,
    producer: KafkaProducer = Depends(get_kafka_producer),
):
    try:
        # Calculate order total
        order_total = sum(item.price * item.quantity for item in order.items)

        # Prepare order message
        order_data = order.dict()
        order_data["total"] = order_total
        order_data["status"] = "created"
        order_data["created_at"] = datetime.now().isoformat()

        # Send order to Kafka
        background_tasks.add_task(
            producer.produce,
            topic=ORDERS_TOPIC,
            value=order_data,
            key=order.order_id,
            headers={"source": "producer-service"},
        )

        # Log order creation
        logger.info(
            "Order created",
            order_id=order.order_id,
            customer_id=order.customer_id,
            total=order_total,
        )

        # Also send an analytics event
        analytics_event = {
            "event_type": "order_created",
            "user_id": order.customer_id,
            "properties": {
                "order_id": order.order_id,
                "total": order_total,
                "items_count": len(order.items),
            },
            "timestamp": datetime.now().isoformat(),
        }

        background_tasks.add_task(
            producer.produce,
            topic=ANALYTICS_TOPIC,
            value=analytics_event,
            key=order.customer_id,
        )

        return {
            "order_id": order.order_id,
            "status": "processing",
            "message": "Order received and is being processed",
        }

    except Exception as e:
        logger.error("Failed to process order", error=str(e), order_id=order.order_id)
        raise HTTPException(status_code=500, detail="Failed to process order")


# Notification sending endpoint
@app.post("/api/notifications", status_code=201)
async def send_notification(
    notification: NotificationRequest,
    background_tasks: BackgroundTasks,
    producer: KafkaProducer = Depends(get_kafka_producer),
):
    try:
        # Prepare notification message
        notification_data = notification.dict()
        notification_data["sent_at"] = datetime.now().isoformat()
        notification_data["status"] = "queued"

        # Send notification to Kafka
        background_tasks.add_task(
            producer.produce,
            topic=NOTIFICATIONS_TOPIC,
            value=notification_data,
            key=notification.recipient_id,
            headers={"priority": notification.priority},
        )

        logger.info(
            "Notification queued",
            notification_id=notification.notification_id,
            recipient_id=notification.recipient_id,
            type=notification.notification_type,
        )

        return {
            "notification_id": notification.notification_id,
            "status": "queued",
            "message": "Notification has been queued for delivery",
        }

    except Exception as e:
        logger.error(
            "Failed to queue notification",
            error=str(e),
            notification_id=notification.notification_id,
        )
        raise HTTPException(status_code=500, detail="Failed to queue notification")


# Analytics event tracking endpoint
@app.post("/api/analytics/events", status_code=201)
async def track_event(
    event: AnalyticsEvent,
    background_tasks: BackgroundTasks,
    producer: KafkaProducer = Depends(get_kafka_producer),
):
    try:
        # Send analytics event to Kafka
        event_data = event.dict()

        background_tasks.add_task(
            producer.produce,
            topic=ANALYTICS_TOPIC,
            value=event_data,
            key=event.user_id if event.user_id else event.event_id,
        )

        logger.info(
            "Analytics event tracked",
            event_id=event.event_id,
            event_type=event.event_type,
        )

        return {
            "event_id": event.event_id,
            "status": "tracked",
            "message": "Event has been tracked successfully",
        }

    except Exception as e:
        logger.error(
            "Failed to track analytics event", error=str(e), event_id=event.event_id
        )
        raise HTTPException(status_code=500, detail="Failed to track analytics event")


# Bulk events tracking endpoint
@app.post("/api/analytics/bulk-events", status_code=201)
async def track_bulk_events(
    events: List[AnalyticsEvent],
    background_tasks: BackgroundTasks,
    producer: KafkaProducer = Depends(get_kafka_producer),
):
    try:
        event_ids = []

        for event in events:
            event_data = event.dict()
            event_ids.append(event.event_id)

            background_tasks.add_task(
                producer.produce,
                topic=ANALYTICS_TOPIC,
                value=event_data,
                key=event.user_id if event.user_id else event.event_id,
            )

        logger.info("Bulk analytics events tracked", count=len(events))

        return {
            "event_count": len(events),
            "event_ids": event_ids,
            "status": "tracked",
            "message": f"{len(events)} events have been tracked successfully",
        }

    except Exception as e:
        logger.error("Failed to track bulk analytics events", error=str(e))
        raise HTTPException(
            status_code=500, detail="Failed to track bulk analytics events"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
