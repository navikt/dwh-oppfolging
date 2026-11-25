import json
import os
from typing import Any
from google.cloud.secretmanager import SecretManagerServiceAsyncClient
from pydantic import SecretStr, Secret


async def get_secret(secret_id: str, version: str = "latest") -> SecretStr:
    """Fetch a utf-8 decoded secret using a short-lived client."""
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("Missing required environment variable: GCP_PROJECT_ID")
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    try:
        async with SecretManagerServiceAsyncClient() as client:
            response = await client.access_secret_version(request={"name": name})
    except Exception:
        raise RuntimeError(f"Failed to access GCP secret {secret_id} v.{version}") from None

    return SecretStr(response.payload.data.decode("utf-8"))


async def get_json_secret(secret_id: str, version: str = "latest") -> Secret[dict[str, Any]]:
    """Deserializes the utf-8 decoded secret as a JSON document expecting a dictionary.
    See `get_secret`

    Raises:
        ValueError: If deserialization fails
        TypeError: If the deserialized document is not JSON Obect (dict)
        RuntimeError: If the GCP Secret Manager API call fails.
    """
    secret = await get_secret(secret_id, version)
    try:
        parsed = json.loads(secret.get_secret_value())
    except json.JSONDecodeError:
        raise ValueError(f"Secret {secret_id} is not valid JSON") from None

    if not isinstance(parsed, dict):
        raise TypeError(f"Expected a JSON Object (dict) in secret {secret_id}")
    return Secret(parsed)



