"oracle api"

import logging
from typing import Generator, Any
from datetime import datetime, timedelta

from oracledb.connection import Connection # pylint: disable=no-name-in-module
from oracledb.connection import connect # pylint: disable=no-name-in-module
from oracledb.cursor import Cursor
from oracledb.var import Var
from oracledb import TIMESTAMP

from dwh_oppfolging.apis.secrets_api_v1 import get_oracle_secrets_for
from dwh_oppfolging.apis.oracle_api_v1_types import Row


def _fix_timestamp_inputtypehandler(cur: Cursor, val: Any, arrsize: int) -> Var | None:
    if isinstance(val, datetime) and val.microsecond > 0:
        return cur.var(TIMESTAMP, arraysize=arrsize) # pylint: disable=no-member
    # No return value implies default type handling
    return None


def create_oracle_connection(username: str) -> Connection:
    """use in with statement
    returns oracle connection with db access
    you have to call .commit() yourself
    """
    con = connect(**get_oracle_secrets_for(username))
    con.inputtypehandler = _fix_timestamp_inputtypehandler
    return con


def log_etl(
    cur: Cursor,
    schema: str,
    table: str,
    etl_date: datetime,
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    rows_deleted: int | None= None,
    log_text: str| None = None,
) -> None:
    """
    inserts into logging table, does not commit
    """
    sql = f"insert into {schema}.etl_logg select :0,:1,:2,:3,:4,:5 from dual"
    cur.execute(sql, [table, etl_date, rows_inserted, rows_updated, rows_deleted, log_text])
    logging.info(f"logged etl for {table}")


def get_table_row_count(cur: Cursor, schema: str, table: str) -> int:
    """
    returns number of rows in table
    """
    sql = f"select count(*) from {schema}.{table}"
    count: int = cur.execute(sql).fetchone()[0] # type: ignore
    return count


def is_table_empty(cur: Cursor, schema: str, table: str) -> bool:
    """
    returns true if table has no rows
    """
    return get_table_row_count(cur, schema, table) == 0


def is_table_stale(
    cur: Cursor,
    schema: str,
    table: str,
    max_hourse_behind_today: int = 72,
    insert_date_column: str = "lastet_dato",
) -> bool:
    """
    returns true if table insert date is too old
    """
    cur.execute(f"select max({insert_date_column}) from {schema}.{table}")
    insert_date: datetime | None = cur.fetchone()[0] # type: ignore
    if insert_date is None:
        return True
    return (datetime.today() - insert_date) >= timedelta(hours=max_hourse_behind_today)


def is_workflow_stale(
    cur: Cursor,
    table_name: str,
    max_hourse_behind_today: int = 24,
) -> bool:
    """
    returns true if last workflow did not succeed or is too old
    """
    cur.execute(
        """
        with t as (
            select
                c.workflow_id workflow_id
                , trunc(c.end_time) updated
                , decode(c.run_err_code, 0, 1, 0) succeeded
                , row_number() over(partition by c.workflow_id order by c.end_time desc) rn
            from
                osddm_report_repos.mx_rep_targ_tbls a
            left join
                osddm_report_repos.mx_rep_sess_tbl_log b
                on a.table_id = b.table_id
            left join
                osddm_report_repos.mx_rep_wflow_run c
                on b.workflow_id = c.workflow_id
            where
                a.table_name = upper(:table_name)
        )
        select * from t where t.rn = 1
        """,
        table_name=table_name # type: ignore
    )
    try:
        row: tuple = cur.fetchone() # type: ignore
        wflow_date: datetime = row[1]
        succeeded = bool(row[2])
    except (TypeError, IndexError) as exc:
        raise Exception(f"Workflow with target {table_name} not found") from exc
    if not succeeded:
        return False
    return (datetime.today().date() - wflow_date.date()) >= timedelta(hours=max_hourse_behind_today)


def execute_stored_procedure(
    cur: Cursor,
    schema: str,
    package: str,
    procedure: str,
    *args, **kwargs,
) -> None:
    """
    execute stored psql procedure
    """
    name = ".".join((schema, package, procedure))
    cur.callproc(name, parameters=args, keyword_parameters=kwargs)


def update_table_from_sql(
    cur: Cursor,
    schema: str,
    table: str,
    update_sql: str,
    bind_today_to_etl_date: bool = True,
    etl_date_bind_name: str = "etl_date",
    enable_etl_logging: bool = True,
) -> tuple[int, int]:
    """
    basic update of table using provided sql
    if bind_today_to_etl_date is set then today() is bound to variable :etl_date_bind_name
    (default: etl_date), note that some bind names like "date" cannot be used.
    """
    today = datetime.today()
    num_rows_old = get_table_row_count(cur, schema, table)
    if bind_today_to_etl_date:
        cur.execute(update_sql, {etl_date_bind_name: today})
    else:
        cur.execute(update_sql)
    rows_affected = cur.rowcount
    num_rows_new: int = get_table_row_count(cur, schema, table)
    rows_inserted = num_rows_new - num_rows_old
    rows_deleted = 0
    if rows_inserted < 0:
        rows_inserted, rows_deleted = rows_deleted, -rows_inserted
    rows_updated = rows_affected - rows_inserted
    logging.info(f"inserted {rows_inserted} new rows")
    logging.info(f"updated {rows_updated} existing rows")
    logging.info(f"deleted {rows_deleted} rows")
    if enable_etl_logging:
        log_etl(cur, schema, table, today, rows_inserted, rows_updated, rows_deleted)
    return rows_inserted, rows_updated


def build_insert_sql_string(
    schema: str,
    table: str,
    cols: list[str],
    unique_columns: list[str] | None = None,
    additional_where_clauses: list[str] | None = None,
) -> str:
    """
    returns a formattable sql insert, optionally with filter columns,
    where rows are not inserted if rows in the target
    with the same column values already exist.
    target table columns are formatted with targ_cols,
    bind columns (values to insert) are formatted with bind_cols
    NB: additional where clauses must not use the 'where' keyword
    >>> build_insert_sql_string('a', 'b', ['x', 'y'], ['x', 'y'])
    'insert into a.b targ (targ.x, targ.y) select :x, :y from dual src where not exists (select null from a.b t where t.x = :x and t.y = :y)'
    >>> build_insert_sql_string('a', 'b', ['x', 'y'], ['x', 'y'], ["2 = 3", "5 = 4"])
    'insert into a.b targ (targ.x, targ.y) select :x, :y from dual src where not exists (select null from a.b t where t.x = :x and t.y = :y) and 2 = 3 and 5 = 4'
    >>> build_insert_sql_string('a', 'b', ['x', 'y'], None, ["2 = 3", "5 = 4"])
    'insert into a.b targ (targ.x, targ.y) select :x, :y from dual src where 2 = 3 and 5 = 4'
    """
    targ_cols = ", targ.".join(cols)
    bind_cols = ", :".join(cols)
    sql = (
        f"insert into {schema}.{table} targ (targ.{targ_cols}) select :{bind_cols} from dual src"
    )
    where_set = False
    if unique_columns is not None and len(unique_columns) > 0:
        sql += (
            f" where not exists (select null from {schema}.{table} t where "
            + " and ".join(f"t.{col} = :{col}" for col in unique_columns)
            + ")"
        )
        where_set = True
    if additional_where_clauses is not None and len(additional_where_clauses) > 0:
        if not where_set:
            sql += " where "
        else:
            sql += " and "
        sql += " and ".join(clause for clause in additional_where_clauses)
    return sql


def _insert_to_table_gen(
    cur: Cursor,
    schema: str,
    table: str,
    data: Generator[list[Row] | Row, None, None] | list[Row] | Row,
    unique_columns: list[str] | None = None,
    additional_where_clauses: list[str] | None = None,
    enable_etl_logging: bool = False,
    continue_on_db_errors: bool = False
):
    # check if data is Row
    if isinstance(data, dict):
        data = [data]

    # insert data
    insert_sql = ""
    rows_inserted = 0
    for item in data:
        if not isinstance(item, list):
            item = [item]
        elif len(item) == 0:
            continue
        if not insert_sql:
            cols = [*(item[0])]
            insert_sql = build_insert_sql_string(schema, table, cols, unique_columns, additional_where_clauses)
        cur.executemany(insert_sql, item, batcherrors=continue_on_db_errors)
        batcherrors = cur.getbatcherrors() or []
        rows_inserted += cur.rowcount
        yield (cur.rowcount, batcherrors)
    if enable_etl_logging:
        log_etl(cur, schema, table, datetime.today(), rows_inserted)


def insert_to_table(
    cur: Cursor,
    schema: str,
    table: str,
    data: Generator[list[Row] | Row, None, None] | list[Row] | Row,
    unique_columns: list[str] | None = None,
    additional_where_clauses: list[str] | None = None,
    enable_etl_logging: bool = True,
):
    """
    Inserts data into table. No commits are made.
    Data can be a row, list of rows, or a generator of either.
    Returns number of rows inserted.

    `unique_columns`: if provided, this combination of columns must be unique for 
    each row to be inserted, or it is skipped. Default: None

    `enable_etl_logging`: if set, metadata will be inserted into the etl logging table
    at the end of data insertion. Default: True
    """
    rows_inserted = 0
    for info in _insert_to_table_gen(
        cur,
        schema,
        table, data,
        unique_columns,
        additional_where_clauses,
        enable_etl_logging
    ):
        rows_inserted += info[0]
    return rows_inserted


def create_table_insert_generator(
    cur: Cursor,
    schema: str,
    table: str,
    data: Generator[list[Row] | Row, None, None] | list[Row] | Row,
    unique_columns: list[str] | None = None,
    additional_where_clauses: list[str] | None = None,
    enable_etl_logging: bool = True,
    continue_on_db_errors: bool = False,
):
    """
    Creates a generator that inserts data into a table. No commits are made.

    Data can be a row, list of rows, or a generator of either.

    The generator yields tuples of insert information 
    (rows inserted, [error]). Errors are only returned if `continue_on_db_errors` is set,
    otherwise the list is empty. The error objects are tuples of
    (batch_index, errcode, message).

    `unique_columns`: if provided, this combination of columns must be unique for 
    each row to be inserted, or it is skipped. Default: None

    `enable_etl_logging`: if set, metadata will be inserted into the etl logging table
    at the end of data insertion. Default: True

    `continue_on_db_errors`: if set, then ORA errors are yielded at each batch and insertion
    is allowed to continue, rather than throwing an exception. Otherwise, only array dml
    counts are returned. Default: False
    NOTE: DPY errors are still thrown, for example when trying to insert a string into
    a number column.
    """
    for info in _insert_to_table_gen(
        cur,
        schema,
        table,
        data,
        unique_columns,
        additional_where_clauses,
        enable_etl_logging,
        continue_on_db_errors
    ):
        yield info
