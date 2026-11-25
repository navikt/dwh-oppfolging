from contextlib import asynccontextmanager
from typing import Any
from datetime import datetime
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from itertools import batched
from oracledb import (
    connect_async, AsyncConnection, is_simple_sql_name, AsyncCursor,
    DB_TYPE_TIMESTAMP, DB_TYPE_TIMESTAMP_TZ
)
from dwh_oppfolging.secret import get_json_secret


Rows = Iterable[dict[str, Any]] | AsyncIterable[dict[str, Any]]


async def _asyncbatched(rows: AsyncIterable[dict[str, Any]], size: int) -> AsyncIterator[list[dict[str, Any]]]:
    buf: list[dict[str, Any]] = []
    async for row in rows:
        buf.append(row)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


async def _chunks(rows: Rows, size: int) -> AsyncIterator[list[dict[str, Any]]]:
    """Yields lists of at most `size` rows from a sync or async iterable.
    
    NOTE: as lists because there is no documented tuple support in executemany
    """
    assert size > 0
    if isinstance(rows, AsyncIterable):
        async for batch in _asyncbatched(rows, size):
            yield batch
    else:
        for batch in batched(rows, size):
            yield list(batch)


def _fix_timestamp_inputtypehandler(cur: AsyncCursor, val, arrsize: int):
    """
    fallback to try to avoid precision loss in datetime types when not
    preparing inputsizes, will only have an effect if the first row
    in the batch doesnt have None in the datetime column
    """
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return cur.var(DB_TYPE_TIMESTAMP, arraysize=arrsize)
        else:
            return cur.var(DB_TYPE_TIMESTAMP_TZ, arraysize=arrsize)
    # No return value implies default type handling
    return None


async def _get_insert_spec(cur: AsyncCursor, row: dict[str, Any], table: str) -> tuple[str, dict[str, Any]]:
    """Returns the insert statement and input sizes from target table column types.
    Names are validated to prevent SQL injection."""
    cols = [k.lower() for k in row]
    for name in (table, *cols):
        if not is_simple_sql_name(name):
            raise ValueError(f"Invalid sql name: {name}")
    await cur.execute(f"select * from {table} where 1 = 0")
    assert cur.description
    coltypes = {d.name.lower(): d.type_code for d in cur.description}
    # columns not in table get None; oracle raises ORA-00904 on insert anyway
    sizes = {c: coltypes.get(c) for c in cols}
    sql = f"insert into {table} ({','.join(cols)}) values ({','.join(':' + c for c in cols)})"
    return sql, sizes


@asynccontextmanager
async def create_oracle_connection(secret_id: str, user: str, proxy: bool = False):
    secret = await get_json_secret(secret_id)
    async with connect_async(
        # TODO: reorganize secrets
        user=secret.get_secret_value()["schemas"][user]["user"] + (f"[{user}]" if proxy else ""),
        password=secret.get_secret_value()["schemas"][user]["pwd"],
        host=secret.get_secret_value()["dsn"]["host"],
        port=secret.get_secret_value()["dsn"]["port"],
        service_name=secret.get_secret_value()["dsn"]["service"]
    ) as con:
        con.inputtypehandler = _fix_timestamp_inputtypehandler
        yield con


async def insert_rows(
    con: AsyncConnection,
    rows: Rows,
    table: str,
    batch_size: int = 1000,
) -> int:
    """Insert rows to the table in batches, guessing inputsizes from the table.
    Accepts any sync or async iterable of dicts; at most batch_size rows are held in memory.
    Assumes each row has the same columns as the first.
    Does not commit; the caller owns the transaction.
    Returns the number of rows inserted."""
    if batch_size < 1:
        raise ValueError("batch size must be at least 1")
    inserted = 0
    async with con.cursor() as cur:
        sql: str | None = None
        sizes: dict[str, Any] = {}
        async for chunk in _chunks(rows, batch_size):
            if sql is None:
                sql, sizes = await _get_insert_spec(cur, chunk[0], table)
            cur.setinputsizes(**sizes)
            await cur.executemany(sql, chunk)
            inserted += cur.rowcount
    return inserted
