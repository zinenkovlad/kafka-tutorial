import time
import avro.schema
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

time.sleep(20)
print('20 seconds passed')

schema = avro.schema.parse(open("record.avsc").read())
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=str(schema)
)

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'Foo',
    'key.deserializer': avro_deserializer,
    'value.deserializer': avro_deserializer
}
consumer = DeserializingConsumer(consumer_conf)

consumer.subscribe(['test-topic'])

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            print(msg.value())
    except KeyboardInterrupt:
        break