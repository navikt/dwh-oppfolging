import os
import subprocess
from contextlib import contextmanager
from typing import Generator
from dwh_oppfolging.apis.secrets_api_v1 import get_oracle_user_credentials
import logging


@contextmanager
def create_dbt_oracle_context(schema: str) -> Generator[None, None, None]:
    """
    TODO: not concurrency safe! need to pass dbt env params to the subprocess directly

    Creates a dbt context setting environment variables used by dbt profile.
    Use in a 'with' statement

    params:
        - schema, str: the schema the dbt project operates in.
    
    yields:
        - None
    """
    creds = get_oracle_user_credentials(schema)
    dbt_env_params = {
        "DBT_ENV_SECRET_USER": creds.get_secret_value()["user"] + f"[{schema}]",
        "DBT_ENV_SECRET_PASS": creds.get_secret_value()["pwd"],
        "DBT_ENV_SECRET_HOST": creds.get_secret_value()["host"],
        "DBT_ENV_SECRET_PORT": creds.get_secret_value()["port"],
        "DBT_ENV_SECRET_SERVICE": creds.get_secret_value()["service"],
        "DBT_ENV_SECRET_DATABASE": creds.get_secret_value()["database"],
        "DBT_ENV_SECRET_SCHEMA": schema,
        "ORA_PYTHON_DRIVER_TYPE": "thin",
    }
    for val in dbt_env_params.values():
        assert isinstance(val, str), "All dbt env var values must be strings"
    os.environ.update(dbt_env_params)
    try:
        yield
    finally:
        for key in dbt_env_params:
            os.environ.pop(key, None)
