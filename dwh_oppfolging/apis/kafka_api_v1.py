"kafka api"
from contextlib import contextmanager
import requests # type: ignore
from dwh_oppfolging.apis.secrets_api_v1 import get_kafka_secrets
from dwh_oppfolging.apis.kafka_api_v1_types import KafkaConnection


def is_dwh_consumer_alive(name: str):
    """returns true if dwh-consumer isalive endpoint returns OK"""
    # pylint: disable=no-member
    return requests.get("https://" + name + ".nais.adeo.no/isalive", timeout=10).status_code == requests.codes.ok


@contextmanager
def create_kafka_connection(teamname: str):
    """
    authenticates and connects to kafka, enabling reading from topics
    """
    secrets = get_kafka_secrets(teamname)
    con = KafkaConnection(secrets)
    yield con
    #del con
    #del secrets
