from confluent_kafka import Producer
import json


def json_serializer(data):
    return json.dumps(data)


def delivery_report(err, msg):
    """Callback function to handle message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Setting up the producer configuration
producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})

# Continuously take user input and send to the Kafka topic
while True:
    user_input = input("Enter a number to send (or 'exit' to quit): ")
    if user_input.lower() == 'exit':
        break

    try:
        data = {"number": int(user_input)}
        producer.produce(
            topic='input-topic',
            key=user_input,
            value=json_serializer(data),
            callback=delivery_report
        )
        producer.poll(0)
    except ValueError:
        print("Please enter a valid number.")

# Wait for all messages to be delivered
producer.flush()






