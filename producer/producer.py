import avro.schema
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import time


schema = avro.schema.parse(open("record.avsc").read())
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(
    schema_str=str(schema),
    schema_registry_client=schema_registry_client,
)

producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

i = 0
while True:
    producer.produce(topic='test-topic', value={"Foo": i, "Bar": str(i + 1)})
    print('produced', i + 1, 'record')
    producer.flush(3)
    time.sleep(1)
    i += 1

