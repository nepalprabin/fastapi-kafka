# processor-service/app/main.py
import os
import signal
import sys
import json
from typing import Dict, Any, Callable, List
import threading
from datetime import datetime
import structlog

from kafka_consumer import KafkaConsumer

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()

# Define Kafka topics
ORDERS_TOPIC = "orders"
NOTIFICATIONS_TOPIC = "notifications"
ANALYTICS_TOPIC = "analytics"
PROCESSED_ORDERS_TOPIC = "processed_orders"


class ProcessorService:
    """Service that processes messages from Kafka topics."""

    def __init__(self):
        """Initialize the processor service."""
        self.bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.running = False
        self.consumers = []
        self.threads = []

        # Initialize signal handling for graceful shutdown
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

        logger.info(
            "Processor service initialized", bootstrap_servers=self.bootstrap_servers
        )

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received, stopping consumers")
        self.running = False

        # Close all consumers
        for consumer in self.consumers:
            consumer.close()

        # Wait for all threads to complete
        for thread in self.threads:
            thread.join(timeout=5.0)

        logger.info("Processor service shutdown complete")
        sys.exit(0)

    def start_consumer(
        self,
        topics: List[str],
        group_id: str,
        process_func: Callable[[Dict[str, Any]], None],
    ):
        """Start a consumer for the specified topics with the given processing function.

        Args:
            topics: List of topics to consume from
            group_id: Consumer group ID
            process_func: Function to process each message
        """
        consumer = KafkaConsumer(
            topics=topics,
            group_id=group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        self.consumers.append(consumer)

        # Create and start a thread for this consumer
        thread = threading.Thread(
            target=self._consumer_loop, args=(consumer, process_func), daemon=True
        )
        self.threads.append(thread)
        thread.start()

        logger.info("Consumer started", topics=topics, group_id=group_id)

    def _consumer_loop(
        self, consumer: KafkaConsumer, process_func: Callable[[Dict[str, Any]], None]
    ):
        """Run the consumer loop to process messages.

        Args:
            consumer: KafkaConsumer instance
            process_func: Function to process each message
        """
        self.running = True

        while self.running:
            try:
                message = consumer.consume(timeout=1.0)
                if message:
                    process_func(message)
            except Exception as e:
                logger.error("Error in consumer loop", error=str(e))

    def process_order(self, message: Dict[str, Any]):
        """Process an order message.

        Args:
            message: Kafka message containing order data
        """
        try:
            order = message["value"]
            order_id = order.get("order_id")

            logger.info("Processing order", order_id=order_id)

            # Simulate order processing
            order["status"] = "processed"
            order["processed_at"] = datetime.now().isoformat()

            # In a real implementation, this would include:
            # - Inventory checks
            # - Payment processing
            # - Shipping arrangements
            # - etc.

            logger.info("Order processed successfully", order_id=order_id)

            # In a real implementation, we would produce a message to another topic
            # with the processed order information

        except Exception as e:
            logger.error("Failed to process order", error=str(e), message=message)

    def process_notification(self, message: Dict[str, Any]):
        """Process a notification message.

        Args:
            message: Kafka message containing notification data
        """
        try:
            notification = message["value"]
            notification_id = notification.get("notification_id")
            recipient_id = notification.get("recipient_id")
            notification_type = notification.get("notification_type")

            logger.info(
                "Processing notification",
                notification_id=notification_id,
                recipient_id=recipient_id,
                type=notification_type,
            )

            # Simulate notification delivery
            # In a real implementation, this would include:
            # - Email sending
            # - SMS sending
            # - Push notification
            # - etc.

            notification["status"] = "delivered"
            notification["delivered_at"] = datetime.now().isoformat()

            logger.info(
                "Notification processed successfully", notification_id=notification_id
            )

        except Exception as e:
            logger.error(
                "Failed to process notification", error=str(e), message=message
            )

    def process_analytics_event(self, message: Dict[str, Any]):
        """Process an analytics event message.

        Args:
            message: Kafka message containing analytics event data
        """
        try:
            event = message["value"]
            event_id = event.get("event_id")
            event_type = event.get("event_type")

            logger.info(
                "Processing analytics event", event_id=event_id, event_type=event_type
            )

            # Simulate analytics processing
            # In a real implementation, this would include:
            # - Event aggregation
            # - Metrics calculation
            # - Data warehousing
            # - etc.

            logger.info("Analytics event processed successfully", event_id=event_id)

        except Exception as e:
            logger.error(
                "Failed to process analytics event", error=str(e), message=message
            )

    def run(self):
        """Run the processor service."""
        logger.info("Starting processor service")

        # Start consumers for each topic with appropriate processing functions
        self.start_consumer(
            topics=[ORDERS_TOPIC],
            group_id="order-processors",
            process_func=self.process_order,
        )

        self.start_consumer(
            topics=[NOTIFICATIONS_TOPIC],
            group_id="notification-processors",
            process_func=self.process_notification,
        )

        self.start_consumer(
            topics=[ANALYTICS_TOPIC],
            group_id="analytics-processors",
            process_func=self.process_analytics_event,
        )

        logger.info("Processor service running")

        # Keep the main thread alive
        try:
            while self.running:
                signal.pause()
        except KeyboardInterrupt:
            self.handle_shutdown(None, None)


if __name__ == "__main__":
    processor = ProcessorService()
    processor.run()
