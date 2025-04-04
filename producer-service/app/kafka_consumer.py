# processor-service/app/kafka_consumer.py
import json
import os
import uuid
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger()


class KafkaConsumer:
    """Base Kafka Consumer class with error handling and retry mechanisms."""

    def __init__(
        self,
        topics: List[str],
        group_id: str = None,
        bootstrap_servers: str = None,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
    ):
        """Initialize the Kafka consumer with the given topics and configuration.

        Args:
            topics: List of Kafka topics to subscribe to
            group_id: Consumer group ID (defaults to a UUID if not provided)
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            auto_offset_reset: Where to start consuming from if no offset is stored
            enable_auto_commit: Whether to automatically commit offsets
        """
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.group_id = group_id or f"consumer-group-{uuid.uuid4()}"
        self.topics = topics

        self.consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "max.poll.interval.ms": 300000,  # 5 minutes
            "session.timeout.ms": 30000,  # 30 seconds
            "heartbeat.interval.ms": 10000,  # 10 seconds
        }

        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe(topics)

        logger.info(
            "Kafka consumer initialized",
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            topics=self.topics,
        )

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    def consume(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Consume a single message from the subscribed topics.

        Args:
            timeout: Maximum time to wait for a message in seconds

        Returns:
            Deserialized message value as a dictionary, or None if no message was received
        """
        try:
            msg = self.consumer.poll(timeout)

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    logger.info(
                        "Reached end of partition",
                        topic=msg.topic(),
                        partition=msg.partition(),
                    )
                    return None
                else:
                    # Actual error
                    logger.error("Consumer error", error=msg.error())
                    raise KafkaException(msg.error())

            # Process the message
            try:
                value = json.loads(msg.value().decode("utf-8"))

                # Extract headers if present
                headers = {}
                if msg.headers():
                    headers = {k: v.decode("utf-8") for k, v in msg.headers()}

                # Extract key if present
                key = msg.key().decode("utf-8") if msg.key() else None

                logger.info(
                    "Message consumed",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=key,
                )

                return {
                    "value": value,
                    "key": key,
                    "headers": headers,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp(),
                }

            except json.JSONDecodeError:
                logger.error("Failed to decode message as JSON", topic=msg.topic())
                return None

        except Exception as e:
            logger.error("Error consuming message", error=str(e))
            raise

    def consume_batch(
        self, max_messages: int = 100, timeout: float = 1.0
    ) -> List[Dict[str, Any]]:
        """Consume a batch of messages from the subscribed topics.

        Args:
            max_messages: Maximum number of messages to consume
            timeout: Maximum time to wait for each message in seconds

        Returns:
            List of deserialized messages
        """
        messages = []
        for _ in range(max_messages):
            message = self.consume(timeout)
            if message:
                messages.append(message)
            else:
                # No more messages available
                break
        return messages

    def consume_loop(
        self, process_message: Callable[[Dict[str, Any]], None], timeout: float = 1.0
    ):
        """Start a continuous consumption loop, processing each message with the provided function.

        Args:
            process_message: Function to process each consumed message
            timeout: Maximum time to wait for each message in seconds
        """
        try:
            while True:
                message = self.consume(timeout)
                if message:
                    process_message(message)
        except KeyboardInterrupt:
            logger.info("Consumption loop interrupted")
        finally:
            self.close()

    def commit(self, message: Dict[str, Any] = None):
        """Commit offsets for the consumed message or all consumed messages.

        Args:
            message: Optional specific message to commit offset for
        """
        try:
            if message:
                self.consumer.commit(message=message)
                logger.debug(
                    "Committed specific offset",
                    topic=message["topic"],
                    partition=message["partition"],
                    offset=message["offset"],
                )
            else:
                self.consumer.commit()
                logger.debug("Committed all consumed offsets")
        except KafkaException as e:
            logger.error("Failed to commit offsets", error=str(e))
            raise

    def close(self):
        """Close the consumer connection."""
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error("Error closing consumer", error=str(e))
