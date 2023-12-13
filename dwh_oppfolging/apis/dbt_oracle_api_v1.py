
import os
import subprocess
from contextlib import contextmanager
from typing import Generator
from dwh_oppfolging.apis.secrets_api_v1 import get_dbt_oracle_secrets_for
import logging


@contextmanager
def create_dbt_oracle_context(username: str) -> Generator[None, None, None]:
    """
    use in with statement
    yields nothing
    but sets and unsets environment variables referenced by dbt profile
    """
    config = get_dbt_oracle_secrets_for(username)
    config |= {"ORA_PYTHON_DRIVER_TYPE": "thin"}
    os.environ.update(config)
    yield
    for k in config:
        os.environ.pop(k)


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
