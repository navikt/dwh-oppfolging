# export TESTCONTAINERS_RYUK_DISABLED=true
import inspect
import requests

from testcontainers.kafka import KafkaContainer
from testcontainers.core.network import Network
from testcontainers.generic import ServerContainer

from confluent_kafka.admin import NewTopic
from confluent_kafka import Producer as ProducerClient
from confluent_kafka.schema_registry import Schema

from dwh_oppfolging.apis.kafka_api_v1_types import (
    KafkaConnection,
    AdminClient,
    SchemaRegistryClient,
    bytes_to_sha256_hash,
    _CONFLUENT_HEADER_SIZE,
    _CONFLUENT_MAGIC_BYTE,
    struct,
    fastavro,
    BytesIO,
)


def test_read_batched_messages_from_topic():
    """tests corresponding method in modified KafkaConnection object"""
    with Network() as network:

        with KafkaContainer() \
            .with_network(network) \
            .with_network_aliases("hostname2") \
        as kafka:

            with ServerContainer(
                port=8081,
                image=inspect.signature(KafkaContainer)
                    .parameters["image"].default
                    .replace("cp-kafka", "cp-schema-registry"), # override image to schema registry image
            ) \
                .with_network(network) \
                .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://hostname2:9092") \
                .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry") \
                .with_env("SCHEMA_REGISTRY_DEBUG", "true") \
                .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081") \
            as srv:
                
                # make sure schema registry is responsive
                url = srv._create_connection_url()
                response = requests.get(f"{url}", timeout=5)
                assert response.status_code == 200, "Response status code is not 200"


                # Create the patched KafkaConnection instance
                # The KafkaConnection class is setup to use SSL with registry auth
                # patch the init it so we can use PLAINTEXT without registry auth
                def kafka_connection_init_patch(self, creds: dict):
                    self._admin_config = {"bootstrap.servers": creds["KAFKA_BROKERS"]}
                    self._schema_registry_config = {"url": creds["KAFKA_SCHEMA_REGISTRY"]}
                    self._consumer_config = self._admin_config | {
                        "group.id": "NOT_USED",
                        "auto.offset.reset": "error",
                        "enable.auto.commit": False,
                        "enable.auto.offset.store": False,
                        "api.version.request": True,
                        'enable.partition.eof': True
                    }
                    self._admin_client = AdminClient(self._admin_config)
                    self._schema_registry_client = SchemaRegistryClient(self._schema_registry_config)
                    self._cached_confluent_schemas = {}
                    self._deserializer_map = {
                        "str": self._str_deserializer,
                        "json": self._json_deserializer,
                        "confluent-json": self._confluent_json_deserializer,
                        "confluent-avro": self._confluent_avro_deserializer
                    }
                    self._byteshasher_map = {
                        "str": bytes_to_sha256_hash,
                        "json": bytes_to_sha256_hash,
                        "confluent-json": lambda x: bytes_to_sha256_hash(x[_CONFLUENT_HEADER_SIZE:]),
                        "confluent-avro": lambda x: bytes_to_sha256_hash(x[_CONFLUENT_HEADER_SIZE:]),
                    }
                creds = {}
                creds["KAFKA_BROKERS"] = kafka.get_bootstrap_server()
                creds["KAFKA_SCHEMA_REGISTRY"] = url
                KafkaConnection.__init__ = kafka_connection_init_patch
                con = KafkaConnection(creds)

                # create and register a avro schema
                avro_schema = {
                    "name": "MyRecord",
                    "type": "record",
                    "fields": [
                        {"name": "x", "type": "int"},
                        {"name": "y", "type": "string"},
                        {"name": "z", "type": {"type": "int", "logicalType": "date"}}
                    ]
                }
                schema_id = con._schema_registry_client.register_schema(
                    "MyTestopic-MySchema"
                    , Schema(schema_str=str(avro_schema).replace("'", '"'))
                )
                
                # serialize messages according to this schema
                byteheader = struct.pack(">bI", _CONFLUENT_MAGIC_BYTE, schema_id) # 0 xxxx
                parsed_schema = fastavro.parse_schema(avro_schema)
                with BytesIO() as fo:
                    pos = []
                    fastavro.schemaless_writer(fo, parsed_schema, {"x": 101, "y": "hello", "z": 1})
                    pos.append((0, fo.tell()))
                    fastavro.schemaless_writer(fo, parsed_schema, {"x": 102, "y": "world", "z": 2})
                    pos.append((pos[-1][-1], fo.tell()))
                    fastavro.schemaless_writer(fo, parsed_schema, {"x": 103, "y": "!", "z": 3})
                    pos.append((pos[-1][-1], fo.tell()))
                    serialized_messages = [byteheader + fo.getvalue()[a:b] for a,b in pos]

                # create a producer, this is not part of the connection object
                producer = ProducerClient(
                    {"bootstrap.servers": creds["KAFKA_BROKERS"]}
                )
                # create a new topic with 2 partitions
                promises = con._admin_client.create_topics(
                    [NewTopic("MyTestTopic", 2)]
                )
                def done(future_object):
                    print("future done", future_object)
                for _, v in promises.items():
                    v.add_done_callback(done)
                    v.result(timeout=5)
                assert "MyTestTopic" in con._admin_client.list_topics().topics
                
                # produce the serialized messages
                msgcount = 0
                def delivered(err, msg):
                    nonlocal msgcount
                    msgcount += 1
                    print("delivered", err, msg)
                for i in range(len(serialized_messages)):
                    producer.produce(
                        "MyTestTopic",
                        key=f"key{i}",
                        value=serialized_messages[i],
                        partition=int(i == 2),
                        on_delivery=delivered,
                    )
                producer.flush(timeout=5)
                assert msgcount == 3

                # Test reading batched messages from topic
                for batch in con.read_batched_messages_from_topic(
                    "MyTestTopic",
                    expected_key_type="str",
                    expected_value_type="confluent-avro"
                ):
                    assert len(batch) == msgcount
                    batch.sort(key=lambda x: (x["KAFKA_PARTITION"], x["KAFKA_OFFSET"]))
                    r1, r2, r3 = batch
                    assert r1["KAFKA_VALUE"] == '{"x": 101, "y": "hello", "z": "1970-01-02"}'
                    assert r2["KAFKA_VALUE"] == '{"x": 102, "y": "world", "z": "1970-01-03"}'
                    assert r3["KAFKA_VALUE"] == '{"x": 103, "y": "!", "z": "1970-01-04"}'


if __name__ == "__main__":
    test_read_batched_messages_from_topic()
