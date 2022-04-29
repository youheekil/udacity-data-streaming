"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
                "bootstrap.servers" : "PLAINTEXT://localhost:9092, PLAINTEXT://localhost:9093, PLAINTEXT://localhost:9094",
                "group.id": topic_name_pattern, # TODO: DOUBLE CHECK
                "default.topic.config":
            {"auto.offset.reset": "earliest" if offset_earliest else "latest"}
        }

        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Configure the AvroConsumer and subscribe to the topics.
        # Make sure to think about how the `on_assign` callback should be invoked.
        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            partition.offset == OFFSET_BEGINNING # TODO: DOUBLE CHECK (OFFSET_BEGINNING)
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        while True:
            message = self.consumer.poll(timeout = self.consume_timeout) # Poll
            if message is None:
                logger.DEBUG("no message received by consumer")
                return 0 # no message is retrieved
            elif message.error() is not None:
                logger.ERROR(f"error from consumer {message.error()}")
                return 0
            else:

                self.message_handler(message)
                logger.INFO(f"Consumer Message Key :{message})
                return 1 # message is processed

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()