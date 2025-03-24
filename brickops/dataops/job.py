from __future__ import annotations

from typing import TYPE_CHECKING, Any

from brickops.databricks.api import ApiClient
from brickops.databricks.context import DbContext, get_context

if TYPE_CHECKING:
    from databricks.sdk.runtime.dbutils_stub import dbutils as dbutils_type


def run_job_by_name(
    job_name: str, dbutils: dbutils_type | None = None
) -> dict[str, Any]:
    """Run a databricks job by name."""
    db_context = get_context(dbutils)
    job = job_by_name(db_context, job_name=job_name)
    if not job:
        raise ValueError(f"Job {job_name} not found.")

    job_id = job["job_id"]
    return run_job(db_context, job_id=job_id)


def job_by_name(db_context: DbContext, job_name: str) -> dict[str, Any] | None:
    """Get job by name."""
    api_client = ApiClient(db_context.api_url, db_context.api_token)
    return api_client.get_job_by_name(job_name=job_name)


def run_job(db_context: DbContext, job_id: str) -> dict[str, Any]:
    """Run job by job_id."""
    api_client = ApiClient(db_context.api_url, db_context.api_token)
    return api_client.run_job_now(job_id=job_id)
