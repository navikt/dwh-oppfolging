"secrets api, assumes GOOGLE_APPLICATION_CREDENTIALS is set to a service account key file path"
import os
import json
from google.cloud.secretmanager import SecretManagerServiceClient, AccessSecretVersionRequest
from pydantic import SecretStr, Secret


#def make_gsm_secret_path(secret_name: str, version: str = "latest", project_id: str|None = None) -> str:
#    project_id = project_id or os.getenv("GCP_TEAM_PROJECT_ID")
#    assert project_id, "Missing Google Cloud Project ID"
#    return "/".join(("projects", project_id, "secrets", secret_name, "versions", version))


def read_utf8_secret(path: str):
    """Reads and returns a utf-8 encoded secret from Google Cloud SecretManager

    Args:
        path: a fully qualified secret path, i.e. "projects/*/secrets/*/versions/latest"

    Returns:
        SecretStr: the utf-8 decoded secret
    """
    client = SecretManagerServiceClient()
    request = AccessSecretVersionRequest(name=path)
    response = client.access_secret_version(request, timeout=10)
    try:
        return SecretStr(response.payload.data.decode("utf-8"))
    except UnicodeError:
        raise Exception("Unable to decode GSM secret payload") from None


def read_json_utf8_secret(path: str) -> Secret[dict]:
    secret = read_utf8_secret(path)
    try:
        return Secret(json.loads(secret.get_secret_value()))
    except json.JSONDecodeError:
        raise ValueError("Unable to decode GSM secret as json") from None


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
    return read_json_utf8_secret(secret_path)


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
    return get_project_secret(os.environ["GCP_TEAM_PROJECT_ID"], os.environ["GCP_KAFKA_SECRET_NAME"]) # set by user


def get_oracle_user_credentials(schema: str):
    """
    Gets the latest version of the oracle-credentials secret for
    the user with full schema access to the supplied schema.

    params:
        - schema, str: the schema name the user has full access to

    returns:
        - user credentials dict
    """
    secret = get_project_secret(os.environ["GCP_TEAM_PROJECT_ID"], os.environ["GCP_ORACLE_SECRET_NAME"]) # set by user
    creds = Secret({
        "user": secret.get_secret_value()["schemas"][schema]["user"],
        "pwd": secret.get_secret_value()["schemas"][schema]["pwd"],
        "host": secret.get_secret_value()["dsn"]["host"],
        "port": secret.get_secret_value()["dsn"]["port"],
        "service": secret.get_secret_value()["dsn"]["service"],
        "database": secret.get_secret_value()["dsn"]["database"],
    })
   
    #creds = secret.get_secret_value()["dsn"] # host, port, service, database
    #creds |= secret["schemas"][schema] # user, pwd
    return creds


def get_bigquery_user_credentials(user: str = ""):
    """
    Gets the latest version of the bigquery-credentials secret.
    This is actually a service account which we need to impersonate?
    """
    if user:
        raise NotImplementedError
    return get_project_secret(os.environ["GCP_TEAM_PROJECT_ID"], os.environ["GCP_BIGQUERY_SECRET_NAME"]) # set by user



