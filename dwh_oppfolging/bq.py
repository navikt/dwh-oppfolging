"""Async row streaming using the synchronous BigQuery client."""
from typing import Any
from asyncio import to_thread
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter as ScalarQueryParameter
from google.oauth2 import service_account
from dwh_oppfolging.secret import get_json_secret


@asynccontextmanager
async def create_bq_client(secret_id: str):
    secret = await get_json_secret(secret_id)
    credentials = service_account.Credentials.from_service_account_info(
        secret.get_secret_value()
    )
    # synchronous cleanup is acceptable since it just releases local http
    # transport resources
    with Client(credentials=credentials) as client:
        yield client


async def read_bq_rows(
    client: Client,
    query: str,
    config: QueryJobConfig | None = None,
    batch_size: int = 100,
) -> AsyncIterator[dict[str, Any]]:
    """"""
    if batch_size < 1:
        raise ValueError("batch size must be at least 1")
    result = await to_thread(client.query_and_wait, query, job_config=config, page_size=batch_size)
    pages = iter(result.pages)
    while (page := await to_thread(next, pages, None)) is not None:
        for row in page:
            yield dict(row.items())
