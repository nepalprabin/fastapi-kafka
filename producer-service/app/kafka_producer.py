import json
import os
import uuid
from typing import Any, Dict, Optional

from confluent_kafka import Producer
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger()


class KafkaProducer:
    """Base Kafka Producer class"""

    def __init__(self, bootstrap_servers: str = None):
        """Initialize kafka producers

        Args:
        bootstrap_servers: Comma separated list of Kafka broker address
        """
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVER"
        )
        self.producer_config = {
            "bootstrap_servers": self.bootstrap_servers,
            "client.id": f"producer-{uuid.uuid}",
            "acks": "all",  # Wait for all replica to acknowledge
            "retries": 5,  # retry upto 5 times
            "retry.backoff.ms": 200,  # Backoff time between retries
        }
        self.producer = Producer(self.producer_config)
        logger.info(
            "Kafka producer initialized", bootstrap_servers=self.bootstrap_servers
        )

    def delivery_report(self, err, msg):
        """Callback function for delivery reports"""
        if err is not None:
            logger.error("Message delivery failed", error=err, topic=msg.topic())
        else:
            logger.info(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    def produce(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        """Produce a message to the specified Kafka topic with retry mechanism

        Args:
            topic: Kafka topic to produce to
            value: Message value as a dictionary
            key: Optional message key
            headers: Optional message headers
        """

        try:
            # Convert value to JSON
            value_bytes = json.dumps(value).encode("utf-8")

            # Convert key to bytes if provided
            key_bytes = key.encode("utf-8") if key else None

            # Convert headers to Kafka format if provided
            kafka_headers = (
                [(k, v.encode("utf-8")) for k, v in headers.items()]
                if headers
                else None
            )

            # Produce message
            self.producer.produce(
                topic=topic,
                value=value_bytes,
                key=key_bytes,
                headers=kafka_headers,
                callback=self.delivery_report,
            )

            # Serve delivery callbacks from previous produce calls
            self.producer.poll(0)

            logger.info("Message produced", topic=topic, key=key)

        except Exception as e:
            logger.error("Failed to produce message", error=str(e), topic=topic)
            raise

    def flush(self, timeout: float = 10.0):
        """Wait for all messages to be delivered

        Args:
            timeout: Maximum time to wait in seconds
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush timeout")
        else:
            logger.info("All messages flushed successfully")
