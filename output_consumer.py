from confluent_kafka import Consumer, KafkaError
import json

# Setting up the consumer configuration
consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'output-consumer-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['output-topic'])

print("Waiting for messages in output consumer...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Error: {msg.error()}")
            break

    message_data = json.loads(msg.value().decode('utf-8'))
    print(f"Received transformed message: {message_data}")

consumer.close()



