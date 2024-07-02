"Google BigQuery API"

import json
from contextlib import contextmanager
from typing import Generator, TypeVar, cast
from google.cloud.bigquery import Client, QueryJobConfig
from google.oauth2 import service_account
from dwh_oppfolging.apis.secrets_api_v1 import get_bigquery_user_credentials



@contextmanager
def create_google_bigquery_connection(teamname: str):
    """yields bigquery connection Client, use in a with statement"""
    # first we need to get the credentials to the service account which can read
    info = get_bigquery_user_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    yield Client(credentials=credentials)


T = TypeVar("T", dict, tuple, str)
def read_rows_from_query(
    client: Client,
    query: str,
    batch_size: int = 100,
    row_type: type[T] = dict,
    job_config: QueryJobConfig | None = None
):
    """
    Read rows from a bigquery sql query in batches of size batch_size

    NOTE: when using row_type str, json dumps of the rows are returned,
        but this does not support datetime objects, so cast them to strings in the query
        for example using string()

    params:
        client: google.cloud.bigquery.Client
        query: str, DQL statement without ';' at the end
        batch_size: int, number of rows to read at a time (default 100)
        row_type: dict, tuple or str (json dump), type of rows to return (default dict)
            dict gives record style entries, i.e. dict{col1: val1, col2: val2, ...}
            tuple gives oracle style row results i.e. tuple(val1, val2, ...)
            str gives json dumps of the record, like with kafka api
        job_config: google.cloud.bigquery.QueryJobConfig, optional query job configuration
            example:
            ```python
            query="select * from depts where dept_id = @deptName"
            job_config=QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("deptName", "STRING", "Sales")
                ]
            )
            ```
    """
    to_rows = None
    if row_type == dict:
        to_rows = lambda page: [dict(row.items()) for row in page]
    elif row_type == tuple:
        to_rows = lambda page: [row.values() for row in page]
    elif row_type == str:
        to_rows = lambda page: [json.dumps(dict(row.items())) for row in page]
    else:
        raise TypeError("row_type must be dict, tuple or str")
    job = client.query(query, job_config=job_config)
    rowiter = job.result(page_size=batch_size) # wait for job to finish
    for page in rowiter.pages:
        rows = cast(list[T], to_rows(page))
        if rows:
            yield rows


def insert_rows_to_table(
    client: Client,
    table: str,
    rows: list[dict],
    job_config: QueryJobConfig | None = None
):
    """
    Insert rows into a bigquery table

    params:
        client: google.cloud.bigquery.Client
        table: str, table name in format 'project.dataset.table'
        rows: list[dict], list of rows to insert
        job_config: google.cloud.bigquery.QueryJobConfig, optional query job configuration
    """
    table_ref = client.get_table(table)
    errors = client.insert_rows(table_ref, rows, job_config=job_config)
    if errors:
        raise ValueError(f"Errors inserting rows: {errors}")
 