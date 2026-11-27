import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dwh_oppfolging.secret import get_json_secret


@asynccontextmanager
async def create_dbt_oracle_context(secret_id: str, schema: str) -> AsyncGenerator[None, None]:
	"""Set dbt Oracle environment variables and restore previous values on exit.

	Run at most one dbt invocation per process. Use a dedicated Flyte task
	environment with fresh containers or reusable-container concurrency=1.
	Fetch credentials inside the executing task, not its parent task.

	Example:
		```python
		from asyncio import to_thread
		from flyteplugins.dbt import DbtNodeResult, invoke_dbt
		from dwh_oppfolging.dbt import create_dbt_oracle_context

		@env.task(cache="disable")
		async def build_models(secret_id: str, schema: str) -> list[DbtNodeResult]:
			async with create_dbt_oracle_context(secret_id, schema):
				return await to_thread(invoke_dbt, [
					"build", "--project-dir", "dbt-project",
					"--profiles-dir", "dbt-profiles",
				])
		```
	"""
	secret = await get_json_secret(secret_id)
	credentials = secret.get_secret_value()
	user = credentials["schemas"][schema]
	dsn = credentials["dsn"]
	values = {
		"DBT_ENV_SECRET_USER": user["user"] + f"[{schema}]",
		"DBT_ENV_SECRET_PASS": user["pwd"],
		"DBT_ENV_SECRET_HOST": dsn["host"],
		"DBT_ENV_PORT": str(dsn["port"]),
		"DBT_ENV_SECRET_SERVICE": dsn["service"],
		"DBT_ENV_SECRET_DATABASE": dsn["database"],
		"DBT_ENV_SECRET_SCHEMA": schema,
		"ORA_PYTHON_DRIVER_TYPE": "thin",
	}
	previous = {key: os.environ.get(key) for key in values}
	try:
		os.environ.update(values)
		yield
	finally:
		for key, value in previous.items():
			if value is None:
				os.environ.pop(key, None)
			else:
				os.environ[key] = value
