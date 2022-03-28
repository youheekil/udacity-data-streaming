import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:29092"
TOPIC_NAME = "test"


async def produce(topic_name):
    """
    Produces data into the Kafka Topic
    :param topic_name:
    :return:
    """
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        # TODO: Produce a message to the topic
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce
        p.produce(topic_name, f"iteraation {curr_iteration}".encode("utf-8"))

        curr_iteration += 1
        await asyncio.sleep(1)

async def consume(topic_name):
    """
    Consumes data from the kafka Topic
    :param topic_name:
    :return:
    """
    # TODO: Configure the consumer with 'bootstrap.servers' and 'group.id'
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer
    c = Consumer({"bootstrap.servers": BROKER_URL,
                  "group.id": "test-consumer-group"})

    # TODO: Subscribe to the topic
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe
    c.subscribe([topic_name])

    # TODO: Poll for a message
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.poll
    while True:
        message = c.poll(1.0)

    # TODO: Handle the message. Remember that you should
    # 1. check if the message is 'None'
    # 2. check if the message has an error:
    # https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.error
    # 3. if 1 and 2 were false, print the message the key and value
    #   key: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.key
    #   value: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.value
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(1)

async def produce_consume():
    """
    Runs the Producer and Consumer tasks
    :return:
    """
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2

def main():
    """
    runs the exercise
    :return:
    """
    # TODO: Configure the AdminClient with 'bootstrap.servers'
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    # TODO: Create a NewTopic object
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)

    # TODO: Using 'client', create the topic
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.create_topics
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
    # TODO: Using 'client', delete the topic
    # See:  https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.delete_topics
        client.delete_topics([topic])

if __name__ == "__main__":
    main()

