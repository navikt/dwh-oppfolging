"secrets api"
import os
import json
from typing import Any
from google.cloud import secretmanager as _secretmanager


def _get_google_cloud_secret(secret_path: str) -> dict[str, Any]:
    client = _secretmanager.SecretManagerServiceClient()
    resource_name = f"{secret_path}/versions/latest"
    secret = client.access_secret_version(name=resource_name)
    data = secret.payload.data.decode("utf-8")
    secret = json.loads(data)
    assert isinstance(secret, dict), "secrets should be formatted as json dicts"
    return secret


def get_knada_gke_secret():
    """
    Gets the latest version of secret at environment variable KNADA_TEAM_SECRET.
    It must be formatted as a json dict.

    returns:
        - secret dict
    """
    return _get_google_cloud_secret(os.environ["KNADA_TEAM_SECRET"]) # set by KNADA


def get_project_secret(project_id: str, secret_name: str):
    """
    Gets the latest version of project secret.
    It must be formatted as a json dict.

    params:
        - project_id: the google cloud project ID (not the name!)
        - secret_name: the secret name (as shown in cloud console secret manager)

    returns:
        - secret dict
    """
    secret_path = f"projects/{project_id}/secrets/{secret_name}"
    return _get_google_cloud_secret(secret_path)


def _get_kafka_secret():
    return get_project_secret(os.environ["GCP_TEAM_PROJECT_ID"], os.environ["GCP_KAFKA_SECRET_NAME"]) # set by user


def get_kafka_user_credentials(user: str = ""):
    """
    Gets the latest version of the kafka-credentials secret.

    params:
        - user, str: not implemented, do not specify.
    returns:
        - credential dict
    """
    if user:
        raise NotImplementedError
    return _get_kafka_secret()


def _get_oracle_secret():
    return get_project_secret(os.environ["GCP_TEAM_PROJECT_ID"], os.environ["GCP_ORACLE_SECRET_NAME"]) # set by user


def get_oracle_user_credentials(schema: str):
    """
    Gets the latest version of the oracle-credentials secret for
    the user with full schema access to the supplied schema.

    params:
        - schema, str: the schema name the user has full access to

    returns:
        - user credentials dict
    """
    secret = _get_oracle_secret()
    creds = secret["dsn"] # host, port, service, database
    creds |= secret["schemas"][schema] # user, pwd
    return creds
