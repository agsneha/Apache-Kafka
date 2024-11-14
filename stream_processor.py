from confluent_kafka import Consumer, Producer, KafkaError
import json


def process_message(message):
    """Process the message by converting it to an integer and squaring it"""
    try:
        transformed = int(message) ** 2
        return transformed
    except ValueError:
        return None


# Consumer configuration to read messages from input-topic
consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',  # Using IPv4 address explicitly
    'group.id': 'stream-processor-group',
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 100000,
    'heartbeat.interval.ms': 30000
})

# Producer configuration to send messages to output-topic
producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})  # Using IPv4

# Subscribing the consumer to the 'input-topic'
consumer.subscribe(['input-topic'])

print("Stream processor is waiting for messages...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event, continue to poll
            continue
        else:
            print(f"Error: {msg.error()}")
            break

    # Extract and print the received message value
    value = msg.value().decode('utf-8')
    print(f"Stream Processor received: {value}")

    # Process the message and send the result to output-topic
    result = process_message(json.loads(value)["number"])
    if result is not None:
        print(f"Transformed {json.loads(value)['number']} to {result}")
        producer.produce('output-topic', key=str(value), value=json.dumps({"result": result}).encode('utf-8'))
        producer.poll(0)  # Trigger delivery callbacks

# Wait for any outstanding messages to be delivered
producer.flush()
consumer.close()

