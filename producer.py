import avro.schema
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

records = [
    {"Foo": 123, "Bar": "B"},
    {"Foo": None},
    {"Bar": "C"}
]


schema = avro.schema.parse(open("record.avsc").read())
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(
    schema_str=str(schema),
    schema_registry_client=schema_registry_client,
)

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

for record in records:
    producer.produce(
        topic='test-topic',
        value=record
    )
    producer.flush(3)
