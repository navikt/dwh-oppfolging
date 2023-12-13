"secrets api"
import os
import json
from typing import Any
from google.cloud import secretmanager as _secretmanager


def _get_knada_gke_secrets(secret_path: str | None = None, version: str | int = "latest") -> dict[str, Any]:
    "reads and returns knada gcp secrets as a dict"
    client = _secretmanager.SecretManagerServiceClient()
    secret_path = secret_path or os.environ["KNADA_TEAM_SECRET"]
    resource_name = f"{secret_path}/versions/{version}"
    secret = client.access_secret_version(name=resource_name)
    data = secret.payload.data.decode("utf-8") # type: ignore
    return json.loads(data)


def get_oracle_secrets_for(username: str) -> dict[str, str]:
    """reads and returns oracle secrets from knada gke"""
    secrets = _get_knada_gke_secrets()["ORACLE_ONPREM"][os.environ["ORACLE_ENV"]]
    username = username.upper()
    return {
        "user": secrets[username + "_USER"],
        "password": secrets[username + "_PW"],
        "dsn": secrets["DSN"]
    }


def get_dbt_oracle_secrets_for(username: str) -> dict[str, Any]:
    """reads and returns dbt oracle secrets from knada gke"""
    secrets = _get_knada_gke_secrets()["ORACLE_ONPREM"][os.environ["ORACLE_ENV"]]
    username = username.upper()
    dsn = secrets["DSN"]
    return {
        "ORACLE_USER": secrets[username + "_USER"],
        "ORACLE_PASSWORD": secrets[username + "_PW"],
        "ORACLE_DBNAME": secrets["DATABASE"],
        "ORACLE_SERVICE": secrets["SERVICE"],
        "ORACLE_PORT": dsn.split(":")[1].split("/")[0],
        "ORACLE_HOST": dsn.split(":")[0]
    }


def get_kafka_secrets(teamname: str) -> dict[str, Any]:
    """reads and returns team kafka secrets from knada gke"""
    teamname = teamname.upper()
    project_id = _get_knada_gke_secrets()["GCP_PROJECT_IDS"][teamname][os.environ["KAFKA_ENV"]]
    secret_path = f"projects/{project_id}/secrets/kafka-credentials"
    secrets = _get_knada_gke_secrets(secret_path)
    return secrets

