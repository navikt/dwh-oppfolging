"read json from endpoint"


from typing import Any
import requests
from jsonschema import validate # dependency of another package
import subprocess
import logging


def get_json_from_url(url: str, schema: Any = None) -> Any:
    """
    returns json encoded content from url
    optionally validates against json schema
    """
    obj = requests.get(url, timeout=10).json()
    if schema is not None:
        validate(obj, schema=schema)
    return obj


def run_subprocess(args: list, secrets: list[str] = [], timeout: float|None = 3600) -> tuple[int, str]:
    """
    run a subprocess and return the exit code and one of stdout/stderr
    stdout, stderr are captured with subprocess.PIPE so that nothing appears in the logs when it is running
    but this way any secrets in stdout and stderr can be removed before finally outputting to log

    NB: this function does not raise an exception if the subprocess returns a non-zero exit code
        to determine if the subprocess was successful, check the return code in the tuple.
    
    params:
        args: list of command line arguments
        secrets (optional): list of secrets to be removed from stdout and stderrand command line arguments when logging
    """

    def filter_secrets(text: str, secrets: list[str]) -> str:
        """filter secrets from text, replacing with asterisks"""
        for secret in secrets:
            text = text.replace(secret, "********")
        return text

    try:
        logging.info("Running subprocess...")
        p_result = subprocess.run(
            args
            , check=True
            , encoding="utf-8"
            , stdout=subprocess.PIPE
            , stderr=subprocess.PIPE
            , timeout=timeout
        )

    except subprocess.CalledProcessError as exc:
        p_cmd = filter_secrets(str(exc.cmd), secrets)
        p_rcode = exc.returncode
        logging.error(f"Subprocess {p_cmd} returned non-zero exit status {p_rcode}.")
        p_stdout = filter_secrets(exc.stdout, secrets)
        p_stderr = filter_secrets(exc.stderr, secrets)
        logging.error(p_stdout)
        logging.error(p_stderr)
        return p_rcode, p_stderr

    except subprocess.TimeoutExpired as exc:
        p_cmd = filter_secrets(str(exc.cmd), secrets)
        logging.error(f"Subprocess {p_cmd} timed out after {timeout}s.")
        p_stdout = filter_secrets(str(exc.stdout), secrets)
        p_stderr = filter_secrets(str(exc.stderr), secrets)
        logging.error(p_stdout)
        logging.error(p_stderr)
        return -1, p_stderr

    except Exception as exc:
        exc_out = filter_secrets(str(exc), secrets)
        logging.error("Unexpected error!")
        logging.error(exc_out)
        raise Exception from None

    else:
        p_args = filter_secrets(str(p_result.args), secrets)
        logging.info(f"Subprocess {p_args} completed succesfully.")
        p_stdout = filter_secrets(p_result.stdout, secrets)
        logging.info(p_stdout)
        p_rcode = p_result.returncode
        return p_rcode, p_stdout