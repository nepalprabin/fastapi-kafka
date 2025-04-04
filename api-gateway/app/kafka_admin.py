# api-gateway/app/kafka_admin.py
import json
import os
from typing import Any, Dict, List, Optional

from confluent_kafka.admin import AdminClient, NewTopic
import structlog

logger = structlog.get_logger()


class KafkaAdmin:
    """Kafka Admin client for topic management and cluster operations."""

    def __init__(self, bootstrap_servers: str = None):
        """Initialize the Kafka admin client.

        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
        """
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.admin_config = {"bootstrap.servers": self.bootstrap_servers}
        self.admin_client = AdminClient(self.admin_config)
        logger.info(
            "Kafka admin client initialized", bootstrap_servers=self.bootstrap_servers
        )

    def create_topics(self, topics: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Create Kafka topics with the specified configurations.

        Args:
            topics: List of topic configurations, each containing:
                - name: Topic name
                - num_partitions: Number of partitions (default: 1)
                - replication_factor: Replication factor (default: 1)
                - config: Optional topic-level configuration

        Returns:
            Dictionary mapping topic names to creation success status
        """
        new_topics = []
        for topic in topics:
            name = topic["name"]
            num_partitions = topic.get("num_partitions", 1)
            replication_factor = topic.get("replication_factor", 1)
            config = topic.get("config", {})

            new_topics.append(
                NewTopic(
                    name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                    config=config,
                )
            )

        # Create the topics
        futures = self.admin_client.create_topics(new_topics)

        # Wait for each creation to finish and collect results
        results = {}
        for topic, future in futures.items():
            try:
                future.result()  # Wait for completion
                results[topic] = True
                logger.info("Topic created successfully", topic=topic)
            except Exception as e:
                results[topic] = False
                logger.error("Failed to create topic", topic=topic, error=str(e))

        return results

    def delete_topics(self, topics: List[str]) -> Dict[str, bool]:
        """Delete the specified Kafka topics.

        Args:
            topics: List of topic names to delete

        Returns:
            Dictionary mapping topic names to deletion success status
        """
        # Delete the topics
        futures = self.admin_client.delete_topics(topics)

        # Wait for each deletion to finish and collect results
        results = {}
        for topic, future in futures.items():
            try:
                future.result()  # Wait for completion
                results[topic] = True
                logger.info("Topic deleted successfully", topic=topic)
            except Exception as e:
                results[topic] = False
                logger.error("Failed to delete topic", topic=topic, error=str(e))

        return results

    def list_topics(self) -> List[str]:
        """List all topics in the Kafka cluster.

        Returns:
            List of topic names
        """
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topics = list(metadata.topics.keys())
            logger.info("Topics retrieved successfully", count=len(topics))
            return topics
        except Exception as e:
            logger.error("Failed to list topics", error=str(e))
            raise

    def topic_exists(self, topic: str) -> bool:
        """Check if a topic exists in the Kafka cluster.

        Args:
            topic: Topic name to check

        Returns:
            True if the topic exists, False otherwise
        """
        try:
            topics = self.list_topics()
            exists = topic in topics
            logger.info("Topic existence checked", topic=topic, exists=exists)
            return exists
        except Exception as e:
            logger.error("Failed to check topic existence", topic=topic, error=str(e))
            raise
