from __future__ import annotations

from typing import TYPE_CHECKING, Any

from brickops.databricks.api import ApiClient
from brickops.databricks.context import DbContext, get_context

if TYPE_CHECKING:
    from databricks.sdk.runtime.dbutils_stub import dbutils as dbutils_type


def run_pipeline_by_name(
    pipeline_name: str, dbutils: dbutils_type | None = None
) -> dict[str, Any]:
    """Run a databricks DLT pipeline by name."""
    db_context = get_context(dbutils)
    pipeline = pipeline_by_name(db_context, pipeline_name=pipeline_name)
    if not pipeline:
        raise ValueError(f"Pipeline {pipeline_name} not found.")

    pipeline_id = pipeline["pipeline_id"]
    return run_pipeline(db_context, pipeline_id=pipeline_id)


def pipeline_by_name(db_context: DbContext, pipeline_name: str) -> dict[str, Any] | None:
    """Get pipeline by name."""
    api_client = ApiClient(db_context.api_url, db_context.api_token)
    return api_client.get_pipeline_by_name(pipeline_name=pipeline_name)


def run_pipeline(db_context: DbContext, pipeline_id: str) -> dict[str, Any]:
    """Run pipeline by pipeline_id."""
    api_client = ApiClient(db_context.api_url, db_context.api_token)
    return api_client.run_pipeline_now(pipeline_id=pipeline_id)
