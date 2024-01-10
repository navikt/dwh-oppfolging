"datatypes used by kafka api"

from typing_extensions import Any, Literal
from typing import Final, Callable, Generator
import logging
import struct
from io import BytesIO
import fastavro
from fastavro.types import Schema
from confluent_kafka import (
    Consumer as ConsumerClient, TopicPartition, Message,
    TIMESTAMP_NOT_AVAILABLE, TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME,
    OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID,
)
from confluent_kafka.error import KafkaError
from confluent_kafka.admin import (
    AdminClient, ClusterMetadata, TopicMetadata, PartitionMetadata
)
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from dwh_oppfolging.transforms.functions import (
    json_bytes_to_string, bytes_to_string, string_to_json,
    string_to_sha256_hash, bytes_to_sha256_hash, json_to_string
)


Partition = int # >= 0
Offset = int # >= 0
Topic = str
UnixEpoch = int
_LogicalOffset = Literal[OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID] # type: ignore
KafkaRecord = dict[str, str | int | None]
SerializationType = Literal["confluent-json", "confluent-avro", "json", "str"]
_Deserializer = Callable[[bytes], tuple[str, int] | tuple[str, None]]
_TIMESTAMP_DESCRIPTORS_LKP: Final[dict[int, str]] = {
    TIMESTAMP_CREATE_TIME: "SOURCE",
    TIMESTAMP_LOG_APPEND_TIME: "BROKER"
}
_CONFLUENT_MAGIC_BYTE = 0
_CONFLUENT_HEADER_SIZE = 5 # 0 xxxx m..., x:schema id byte, m:message byte
_CONFLUENT_SUBJECT_NOT_FOUND = 40401
_CONFLUENT_VERSION_NOT_FOUND = 40402


# wrap fastavro logical type readers so they return strings
# to improve avro record compatibility with json
from fastavro.logical_readers import LOGICAL_READERS
def _datetime_logical_wrapper(f: Callable):
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs).isoformat()
    return wrapper
for _logical_type in (
    "long-timestamp-millis",
    "long-local-timestamp-millis",
    "long-timestamp-micros",
    "long-local-timestamp-micros",
    "int-date",
    "int-time-millis",
    "long-time-micros",
):
    LOGICAL_READERS[_logical_type] = _datetime_logical_wrapper(LOGICAL_READERS[_logical_type])
del _logical_type


class _DeserializationError(Exception):
    pass


class KafkaConnection:
    """connection class for kafka admin and consumer"""
    def __init__(self, secrets: dict[str, Any]) -> None:
        
        self._admin_config = {
            "bootstrap.servers": secrets["KAFKA_BROKERS"],
            "security.protocol": "SSL",
            "ssl.key.pem": secrets["KAFKA_PRIVATE_KEY"],
            "ssl.certificate.pem": secrets["KAFKA_CERTIFICATE"],
            "ssl.ca.pem": secrets["KAFKA_CA"],
        }

        self._schema_registry_config = {
            "url": secrets["KAFKA_SCHEMA_REGISTRY"],
            "basic.auth.user.info": \
                secrets["KAFKA_SCHEMA_REGISTRY_USER"]
                + ":"
                + secrets["KAFKA_SCHEMA_REGISTRY_PASSWORD"]
        }

        self._consumer_config = self._admin_config | {
            "group.id": "NOT_USED",
            "auto.offset.reset": "beginning",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "api.version.request": True,
            'enable.partition.eof': True
        }

        self._admin_client = AdminClient(self._admin_config)
        self._schema_registry_client = SchemaRegistryClient(self._schema_registry_config)

        self._cached_confluent_schemas: dict[int, Schema] = {}
        self._deserializer_map: dict[SerializationType, _Deserializer] = {
            "str": self._str_deserializer,
            "json": self._json_deserializer,
            "confluent-json": self._confluent_json_deserializer,
            "confluent-avro": self._confluent_avro_deserializer
        }

    def _str_deserializer(self, value: bytes) -> tuple[str, None]:
        try:
            deserialized_value = bytes_to_string(value)
            return deserialized_value, None
        except Exception as exc:
            raise _DeserializationError(*exc.args) from None

    def _json_deserializer(self, value: bytes) -> tuple[str, None]:
        try:
            deserialized_value = json_bytes_to_string(value)
            return deserialized_value, None
        except Exception as exc:
            raise _DeserializationError(*exc.args) from None

    def _extract_confluent_schema_id(self, value: bytes) -> int:
        magic_byte, schema_id = struct.unpack(">bI", value[:_CONFLUENT_HEADER_SIZE])
        assert magic_byte == _CONFLUENT_MAGIC_BYTE
        return schema_id

    def _confluent_json_deserializer(self, value: bytes) -> tuple[str, int]:
        try:
            # JSON documents describe their own schema
            schema_id = self._extract_confluent_schema_id(value)
            deserialized_value = json_bytes_to_string(value[_CONFLUENT_HEADER_SIZE:])
            return deserialized_value, schema_id
        except Exception as exc:
            raise _DeserializationError(*exc.args) from None

    def _confluent_avro_deserializer(self, value: bytes) -> tuple[str, int]:
        try:
            schema_id = self._extract_confluent_schema_id(value)
            try:
                schema = self._cached_confluent_schemas[schema_id]
            except KeyError:
                schema = self.get_confluent_registry_schema_from_id(schema_id)
                self._cached_confluent_schemas[schema_id] = schema
            with BytesIO(value[_CONFLUENT_HEADER_SIZE:]) as fo:
                record = fastavro.schemaless_reader(fo, schema)
            deserialized_value = json_to_string(record)
            return deserialized_value, schema_id
        except Exception as exc:
            raise _DeserializationError(*exc.args) from None

    def _get_partition_lkp(self, topic: Topic) -> dict[Partition, PartitionMetadata]:
        cluster_metadata: ClusterMetadata = self._admin_client.list_topics()
        try:
            topic_metadata: TopicMetadata = cluster_metadata.topics[topic]
        except KeyError:
            raise KeyError(f"Topic {topic} not found.") from None
        if topic_metadata.error is not None:
            raise topic_metadata.error # type: ignore

        for partition_metadata in topic_metadata.partitions.values():
            if partition_metadata.error is not None:
                raise partition_metadata.error
        return topic_metadata.partitions

    def _build_assignable_list_of_topic_partitions(self,
        topic: Topic,
        partition_lkp: dict[Partition, PartitionMetadata],
        default_offset: _LogicalOffset,
        custom_partition_offsets: list[tuple[Partition, Offset]] | None = None,
    ) -> list[TopicPartition]:
        topic_partitions: list[TopicPartition] = []
        custom_lkp: dict[Offset, Partition] = {}
        if custom_partition_offsets is not None:
            custom_partitions = [x[0] for x in custom_partition_offsets]
            custom_offsets = [x[1] for x in custom_partition_offsets]
            custom_lkp = dict(zip(custom_partitions, custom_offsets))
        for partition in partition_lkp:
            offset = custom_lkp.get(partition, default_offset)
            topic_partitions.append(TopicPartition(topic, partition, offset))
        return topic_partitions

    def _unpack_message_into_kafka_record(
        self,
        message: Message,
        key_deserializer: _Deserializer | None,
        value_deserializer: _Deserializer | None,
    ) -> KafkaRecord:

        topic: str | None = message.topic()
        partition: int | None = message.partition()
        offset: int | None = message.offset()

        timestamp_data: tuple[int, int] = message.timestamp()
        timestamp_type = timestamp_data[0]
        # The returned timestamp should be ignored if the timestamp type is TIMESTAMP_NOT_AVAILABLE.
        timestamp_value = timestamp_data[1] if timestamp_type != TIMESTAMP_NOT_AVAILABLE else None
        timestamp_desc = _TIMESTAMP_DESCRIPTORS_LKP.get(timestamp_type)

        headers_raw: list[tuple[str, bytes]] | None = message.headers() # cast bytes to hex string
        headers = ",".join(":".join((h[0], h[1].hex())) for h in headers_raw) if headers_raw else None
        # latency: float | None = message.latency() # (producer only)

        key: str | bytes | None = message.key()
        key_hash: str | None = None
        key_schema_id: int | None = None
        deserialized_key: str | None = None
        if type(key) is bytes:
            key_hash = bytes_to_sha256_hash(key)
            if key_deserializer is not None:
                deserialized_key, key_schema_id = key_deserializer(key)
            else:
                deserialized_key = key.hex()
        elif type(key) is str: # the object is presumably already deserialized
            key_hash = string_to_sha256_hash(key)
            deserialized_key = key

        value: str | bytes | None = message.value()
        value_hash: str | None = None
        value_schema_id: int | None = None
        deserialized_value: str | None = None
        if type(value) is bytes:
            value_hash = bytes_to_sha256_hash(value)
            if value_deserializer is not None:
                deserialized_value, value_schema_id = value_deserializer(value)
            else:
                deserialized_value = value.hex()
        elif type(value) is str: # the object is presumably already deserialized
            value_hash = string_to_sha256_hash(value)
            deserialized_value = value

        return {
            "KAFKA_KEY": deserialized_key,
            "KAFKA_KEY_HASH": key_hash,
            "KAFKA_KEY_SCHEMA": key_schema_id,
            "KAFKA_VALUE": deserialized_value,
            "KAFKA_VALUE_HASH": value_hash,
            "KAFKA_VALUE_SCHEMA": value_schema_id,
            "KAFKA_TOPIC": topic,
            "KAFKA_OFFSET": offset,
            "KAFKA_PARTITION": partition,
            "KAFKA_TIMESTAMP": timestamp_value,
            "KAFKA_TIMESTAMP_TYPE": timestamp_desc,
            "KAFKA_HEADERS": headers
        }

    # public methods
    def get_confluent_registry_schema_from_id(self, schema_id: int) -> Schema:
        """returns the fastavro parsed schema from the global registry id"""
        schema = self._schema_registry_client.get_schema(schema_id)
        parsed_schema = fastavro.parse_schema(string_to_json(schema.schema_str))
        return parsed_schema

    def find_all_confluent_registry_schemas_for_topic(self, topic: Topic) -> dict[int, Schema]:
        """returns a dictionary, possibly empty, of all registered key/value schemas for this topic
        the returned schemas are parsed for use with fastavro before returning
        """
        schema_lkp: dict[int, Schema] = {}
        for field in ("key", "value"):
            try:
                subject = topic + "-" + field
                versions: list[int] = self._schema_registry_client.get_versions(subject)
                for version in versions:
                    version_info = self._schema_registry_client.get_version(subject, version)
                    schema_id: int = version_info.schema_id
                    parsed_schema = self.get_confluent_registry_schema_from_id(schema_id)
                    schema_lkp[schema_id] = parsed_schema
            except SchemaRegistryError as exc:
                assert exc.error_code in (_CONFLUENT_SUBJECT_NOT_FOUND, _CONFLUENT_VERSION_NOT_FOUND)
                continue
        return schema_lkp

    def get_partitions(self, topic: Topic) -> list[Partition]:
        """return list of partitions for topic"""
        return list(self._get_partition_lkp(topic).keys())

    def get_start_and_end_offsets(self, topic: Topic, partition: Partition) -> tuple[Offset, Offset] | None:
        """return tuples of start and end offsets for topic and partition
        note: this creates a temporary consumer to read them"""
        consumer_client = ConsumerClient(self._consumer_config)
        lo_hi_or_none: tuple[int, int] | None = consumer_client.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        consumer_client.close()
        return lo_hi_or_none

    def get_closest_offsets(self, topic: Topic, timestamp: UnixEpoch) -> list[tuple[Partition, Offset | _LogicalOffset]]:
        """returns smallest offsets whose timestamp >= UnixEpoch for each partition
        returned offset will be logical OFFSET_END where the timestamp exceeds that of the last message
        """
        consumer_client = ConsumerClient(self._consumer_config)
        partitions = self.get_partitions(topic)
        topic_partitions = [TopicPartition(topic, partition, timestamp) for partition in partitions]
        topic_partitions = consumer_client.offsets_for_times(topic_partitions)
        consumer_client.close()
        return [(tp.partition, tp.offset if tp.offset > 0 else OFFSET_END) for tp in topic_partitions]

    def read_batched_messages_from_topic(
        self, topic: Topic,
        read_from_end_instead_of_beginning: bool = False,
        expected_key_type: SerializationType | None = None,
        expected_value_type: SerializationType | None = None,
        custom_start_partition_offsets: list[tuple[Partition, Offset]] | None = None,
        batch_size: int = 1000,
        record_callback: Callable[[KafkaRecord], Any] | None = None
    ) -> Generator[list[KafkaRecord], None, None]:
        """
        reads messages from topic beginning (or end)
        or from custom offsets (silently ignored for non-existing partitions)
        """
        
        # try to cache all before message loop
        if "confluent-avro" in (expected_key_type, expected_value_type):
            self._cached_confluent_schemas = self.find_all_confluent_registry_schemas_for_topic(topic)

        key_deserializer = self._deserializer_map.get(expected_key_type) # type: ignore
        value_deserializer = self._deserializer_map.get(expected_value_type) # type: ignore

        default_offset = OFFSET_END if read_from_end_instead_of_beginning else OFFSET_BEGINNING
        partition_lkp = self._get_partition_lkp(topic)
        topic_partitions = self._build_assignable_list_of_topic_partitions(topic, partition_lkp, default_offset, custom_start_partition_offsets)
        del default_offset
        del partition_lkp

        consumer_client = ConsumerClient(self._consumer_config)
        consumer_client.assign(topic_partitions)
        logging.info(f"Assigned to {consumer_client.assignment()}.")
        
        # main loop
        batch: list[KafkaRecord] = []
        empty_counter = 0
        non_empty_counter = 0
        assignment_count = len(consumer_client.assignment())
        while assignment_count > 0:
            
            message: Message | None = consumer_client.poll(timeout=10)
            if message is None:
                empty_counter += 1
                continue
            non_empty_counter += 1

            try:
                err: KafkaError | None = message.error()

                # case: event, error
                if err is not None:
                    if err.fatal(): # not err.retriable() or err.fatal():
                        raise err
                    if err.code() == KafkaError._PARTITION_EOF:
                        err_topic = message.topic()
                        err_partition = message.partition()
                        assert err_topic is not None, "Topic missing in EOF sentinel object"
                        assert err_partition is not None, "Partition missing in EOF sentinel object"
                        consumer_client.incremental_unassign([TopicPartition(err_topic, err_partition)])
                        assignment_count -= 1
                        if assignment_count <= 0:
                            yield batch
                            batch = []
                    else:
                        logging.error(err.str())

                # case: proper message
                else:
                    record = self._unpack_message_into_kafka_record(message, key_deserializer, value_deserializer)
                    if record_callback is not None:
                        record = record_callback(record)
                    batch.append(record)
                    if len(batch) >= batch_size:
                        logging.info("Yielding kafka batch.")
                        consumer_client.pause(consumer_client.assignment())
                        yield batch
                        batch = []
                        consumer_client.resume(consumer_client.assignment())

            except Exception as exc:
                logging.error("Bailing out...")
                consumer_client.close()
                yield batch
                batch = []
                raise exc
        logging.info(f"Completed with {non_empty_counter} events consumed")
        if empty_counter > 0:
            logging.warning(f"found {empty_counter} empty messages")
