"datatypes used by kafka api"

from typing import Final, Callable, Iterator, Any, Literal, get_args
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
_BytesHasher = Callable[[bytes], str]
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
del _logical_type # type: ignore


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
            "auto.offset.reset": "error", # <- Action to take when there is no initial offset in offset store 
            "enable.auto.commit": False,  # ^ or the desired offset is out of range: beginning or end or error
            "enable.auto.offset.store": False,
            "api.version.request": True,
            'enable.partition.eof': True
        }

        self._admin_client = AdminClient(self._admin_config)
        self._schema_registry_client = SchemaRegistryClient(self._schema_registry_config)

        self._cached_confluent_schemas: dict[int, Schema] = {}
        self._deserializer_map: dict[SerializationType | None, _Deserializer] = {
            "str": self._str_deserializer,
            "json": self._json_deserializer,
            "confluent-json": self._confluent_json_deserializer,
            "confluent-avro": self._confluent_avro_deserializer
        }
        self._byteshasher_map: dict[SerializationType | None, _BytesHasher] = {
            "str": bytes_to_sha256_hash,
            "json": bytes_to_sha256_hash,
            "confluent-json": lambda x: bytes_to_sha256_hash(x[_CONFLUENT_HEADER_SIZE:]),
            "confluent-avro": lambda x: bytes_to_sha256_hash(x[_CONFLUENT_HEADER_SIZE:]),
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
            schema_id = self._extract_confluent_schema_id(value)
            deserialized_value = json_bytes_to_string(value[_CONFLUENT_HEADER_SIZE:]) # JSON objects describe their own schema
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
        custom_lkp: dict[Partition, Offset] = {}
        if custom_partition_offsets is not None:
            custom_partitions = [x[0] for x in custom_partition_offsets]
            custom_offsets = [x[1] for x in custom_partition_offsets]
            custom_lkp |= dict(zip(custom_partitions, custom_offsets))
        for partition in partition_lkp:
            offset = custom_lkp.get(partition, default_offset)
            topic_partitions.append(TopicPartition(topic, partition, offset))
        return topic_partitions

    def _unpack_message_into_kafka_record(
        self,
        message: Message,
        key_deserializer: _Deserializer | None,
        value_deserializer: _Deserializer | None,
        key_bytes_hasher: _BytesHasher,
        value_bytes_hasher: _BytesHasher,
    ) -> KafkaRecord:

        topic: str | None = message.topic()
        partition: int | None = message.partition()
        offset: int | None = message.offset()

        timestamp_data: tuple[int, int] = message.timestamp()
        timestamp_type = timestamp_data[0]
        # The returned timestamp should be ignored if the timestamp type is TIMESTAMP_NOT_AVAILABLE.
        # confluent-kafka: The timestamp is the number of milliseconds since the epoch (UTC).
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
            key_hash = key_bytes_hasher(key)
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
            value_hash = value_bytes_hasher(value)
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
        lo_hi_or_none: tuple[Offset, Offset] | None = consumer_client.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
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
        self, topic: Topic, *,
        batch_size: int = 1000,
        expected_key_type: SerializationType | None = None,
        expected_value_type: SerializationType | None = None,
        custom_start_partition_offsets: list[tuple[Partition, Offset]] | None = None,
        record_callback: Callable[[KafkaRecord], Any] | None = None,
        clip_custom_start_offsets: bool = True,
    ) -> Iterator[list[KafkaRecord | Any]]:
        """
        Reads kafka messages from a kafka topic using the assign-poll method,
        yielding proper messages in batches as processed records,
        until the end of all the topic's partitions have been reached.
        Error- and event type messages are merely logged, except for _PARTITION_EOF, which is treated separately for unassigning.
        The number of completely empty messages are also logged (undocumented library case, not the same as tombstones).

        The records are dicts with the following keys:
            - KAFKA_KEY, KAFKA_KEY_HASH, KAFKA_KEY_SCHEMA
            - KAFKA_VALUE, KAFKA_VALUE_HASH, KAFKA_VALUE_SCHEMA
            - KAFKA_TIMESTAMP, KAFKA_TIMESTAMP_TYPE
            - KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
            - KAFKA_HEADERS

        params:
            - topic:
                The full name of the topic.
            - read_from_end_instead_of_beginning: bool (default: False)
                If set, partitions without a given starting offset are read from the end, instead of the beginning.
            - expected_key_type: str | None (default: None)
                Specifies how the key data is deserialized and how its sha256 hash is calculated.
                If left unspecified, data deserialization is not performed, but a string is emitted instead:
                    either a hex-representation of the data bytes
                    or possibly the serialized data itself if it was a string (undocumented library case).
                Hashing is performed on the serialized data, except in the odd string case.
                For confluent-avro and confluent-json, the first 5 data bytes are skipped in hash calculation,
                since these contain metadata regarding the confluent schema separate from the serialized data.
            - expected_value_type: str | None (default: None)
                Same as for the key, but applied to the value data.
            - custom_start_partition_offsets: [(partition int, offset int)] | None (default: None)
                Force starting offsets for given- and existing partitions.
                If a given partition does not exist, it is ignored.
                Offsets for partitions not specified default to OFFSET_BEGINNING
            - batch_size: int (default: 1000)
                The maximum number of messages to consume before yielding a batch
            - record_callback: (dict -> any) | None (default: None)
                Specifices a function to be called on each consumed and processed kafka message (record)
                before batch collection.
            - clip_custom_start_offsets: bool (default: True)
                if set, custom start offsets less than the low watermark offset is set to OFFSET_BEGINNING,
                and custom start offsets greater than the high watermark offset is set to OFFSET_END.
                Otherwise, if not set, an out-of-bounds error may occur if it is not in the watermark range.

                Clipping is enabled by default so that one can request to start reading from say last-commited-offset+1,
                with the intention of continuing to read from where one last stopped, without reading the same
                message over again, and in case there is nothing new, simply ceasing to read.
                Similarly, one may pass an older offset with the intent of re-reading messages, even though
                it may be missing due to log truncation.
                NOTE: In the case that last-commited-offset+1 does not exist because it has been deleted from the partition,
                but is also greater than the high watermark offset, then it follows there is nothing new to read.

        yields:
            - list of records (dicts)
        """
        
        # validate
        assert expected_key_type is None or expected_key_type in get_args(SerializationType)
        assert expected_value_type is None or expected_value_type in get_args(SerializationType)
        if custom_start_partition_offsets is not None:
            for idx, (partition, offset) in enumerate(custom_start_partition_offsets):
                result = self.get_start_and_end_offsets(topic, partition)
                if result is None: # timeout, skip validation, will be caught as error in the msg loop anyway
                    logging.warning(f"custom start offset validation for partition {partition} failed due to timeout")
                else:
                    offset_lo, offset_hi = result
                    if offset < offset_lo or offset > offset_hi:
                        if clip_custom_start_offsets:
                            offset = OFFSET_END if offset > offset_hi else OFFSET_BEGINNING
                            custom_start_partition_offsets[idx] = (partition, offset)
                            logging.info(f"clipped custom start offset for partition {partition}")
                        else:
                            raise Exception(f"custom start offset {offset} on partition {partition} out-of-bounds")

        # try to cache all avro schemas before message loop
        if "confluent-avro" in (expected_key_type, expected_value_type):
            self._cached_confluent_schemas = self.find_all_confluent_registry_schemas_for_topic(topic)

        # set up deserializers
        key_deserializer = self._deserializer_map.get(expected_key_type)
        value_deserializer = self._deserializer_map.get(expected_value_type)
        key_bytes_hasher = self._byteshasher_map.get(expected_key_type, bytes_to_sha256_hash)
        value_bytes_hasher = self._byteshasher_map.get(expected_value_type, bytes_to_sha256_hash)

        # build topic-partition-offset assign list
        default_offset = OFFSET_BEGINNING # OFFSET_END
        partition_lkp = self._get_partition_lkp(topic)
        topic_partitions = self._build_assignable_list_of_topic_partitions(topic, partition_lkp, default_offset, custom_start_partition_offsets)
        del default_offset
        del partition_lkp

        consumer_client = ConsumerClient(self._consumer_config)
        consumer_client.assign(topic_partitions)
        logging.info(f"Assigned to {consumer_client.assignment()}.")
        
        # setup main loop
        batch: list[KafkaRecord | Any] = []
        empty_counter = 0
        non_empty_counter = 0
        assignment_count = len(consumer_client.assignment())

        # main loop
        while assignment_count > 0:

            message: Message | None = consumer_client.poll(timeout=10)
            if message is None:
                empty_counter += 1
                continue
            non_empty_counter += 1

            try:
                err: KafkaError | None = message.error() #  None: proper message, KafkaError: event or error 

                # if: event, error
                if err is not None:

                    # see librdkafka introduction.md: consumer should be destroyed when error is fatal
                    if err.fatal():
                        raise err

                    # handle non-fatal event/error
                    match err.code():

                        # Broker: No more messages
                        case KafkaError._PARTITION_EOF:
                            err_topic, err_partition = message.topic(), message.partition()
                            assert err_topic is not None and err_partition is not None
                            logging.info(f"reached end of partition {err_partition}")
                            consumer_client.incremental_unassign([TopicPartition(err_topic, err_partition)])

                            # unassign from finished partition
                            assignment_count -= 1
                            if assignment_count <= 0:
                                logging.info("reached end of all partitions")
                                yield batch
                                batch = []

                        # Local: No offset to automatically reset to
                        # In python confluent-kafka lib this may happen when we give an
                        # invalid starting offset, out-of-range. However, we can only catch it with
                        # auto.offset.reset='error', that is, as an offset reset error (why?).
                        # Any offset outside of low watermark and high watermark range will trigger the error.
                        # Presumably this will also happen if the requested offset doesnt exist,
                        # even if it is within the range, as the message reads:
                        # (fetch failed due to requested offset not available on the broker).
                        case KafkaError._AUTO_OFFSET_RESET:
                            err_topic, err_partition, err_offset = message.topic(), message.partition(), message.offset()
                            assert err_topic is not None and err_partition is not None and err_offset is not None
                            logging.error(f"start offset {err_offset} on partition {err_partition} can not be fetched")
                            logging.error("consider using the closest one (in time) from `get_closest_offsets` instead")
                            raise err

                        # unhandled errors
                        case _:
                            raise err

                # case: proper message
                else:
                    record = self._unpack_message_into_kafka_record(
                        message, key_deserializer, value_deserializer, key_bytes_hasher, value_bytes_hasher
                    )
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

        logging.info(f"Completed with {non_empty_counter} messages consumed")
        if empty_counter > 0:
            logging.warning(f"found {empty_counter} empty messages")
