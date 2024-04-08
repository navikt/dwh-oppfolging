
import os
import subprocess
from contextlib import contextmanager
from typing import Generator
from dwh_oppfolging.apis.secrets_api_v1 import get_oracle_user_credentials
import logging


@contextmanager
def create_dbt_oracle_context(schema: str) -> Generator[None, None, None]:
    """
    Creates a dbt context setting environment variables used by dbt profile.
    Use in a 'with' statement

    params:
        - schema, str: the schema the dbt project operates in.
    
    yields:
        - None
    """
    creds = get_oracle_user_credentials(schema)
    dbt_env_params = {
        "DBT_ENV_SECRET_USER": creds["user"] + f"[{schema}]",
        "DBT_ENV_SECRET_PASS": creds["pwd"],
        "DBT_ENV_SECRET_HOST": creds["host"],
        "DBT_ENV_SECRET_PORT": creds["port"],
        "DBT_ENV_SECRET_SERVICE": creds["service"],
        "DBT_ENV_SECRET_DATABASE": creds["database"],
        "DBT_ENV_SECRET_SCHEMA": schema,
        "ORA_PYTHON_DRIVER_TYPE": "thin",
    }
    os.environ.update(dbt_env_params)
    yield
    for key in dbt_env_params:
        os.environ.pop(key)


def execute_dbt_project(command: str, profiles_dir: str, project_dir: str, *args):
    """
    executes dbt command as subprocess
    assuming profiles yaml file is located
    this should be done inside a dbt_oracle context
    for ordinary test+run use command: 'build'
    for tests only, use: 'test'
    for running only, use: 'run'

    params:
        command: str, name of command
        profiles_dir: str, directory containing profile.yml
        project_dir: str, directory containing project
        *args: any additional dbt command options
    """
    try:
        completed_proc = subprocess.run(
            ["dbt", command, "--profiles-dir", profiles_dir, "--project-dir", project_dir, *args],
            check=True, capture_output=True, encoding="utf-8"
        )
    except subprocess.CalledProcessError as exc:
        errtext = exc.stdout + "\n" + exc.stderr
        raise Exception(errtext) from exc
    else:
        log = logging.getLogger()
        log.info("Process completed with return code " + str(completed_proc.returncode))
        log.info(completed_proc.stdout)
