"read json from endpoint"


from typing import Any
import requests
from jsonschema import validate # dependency of another package


def get_json_from_url(url: str, schema: Any = None) -> Any:
    """
    returns json encoded content from url
    optionally validates against json schema
    """
    obj = requests.get(url, timeout=10).json()
    if schema is not None:
        validate(obj, schema=schema)
    return obj
