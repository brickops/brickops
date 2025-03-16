import logging
from typing import NamedTuple

from brickops.databricks.api import ApiClient
from brickops.databricks.context import get_context
from brickops.databricks.username import get_username

logger = logging.getLogger(__name__)


class Job(NamedTuple):
    """Represents a job in Databricks."""

    name: str
    id: str


def get_api_client() -> ApiClient:
    """Create an API client for Databricks."""
    context = get_context()
    return ApiClient(context.api_url, context.api_token)


def get_jobs(api_client: ApiClient) -> list[Job]:
    context = get_context()
    username = get_username(context)
    jobs = api_client.get_jobs()
    return [
        Job(job["settings"]["name"], job["job_id"])
        for job in jobs
        if "tags" in job["settings"]
        and username in job["settings"]["tags"]["deployment"]
    ]


def delete_jobs(api_client: ApiClient, jobs: list[Job]) -> None:
    for job in jobs:
        logger.info(f"Deleting job '{job.name}' with job_id={job.id}")
        api_client.delete_job(job.id)


def get_schemas(api_client: ApiClient) -> list[str]:
    """Get all schemas that contain the username of the current user."""
    context = get_context()
    username = get_username(context)
    catalogs = api_client.get_catalogs()
    schemas = []
    for catalog in catalogs:
        schemas_in_catalog = api_client.get_schemas(catalog["name"])
        schemas.extend(
            [
                schema["full_name"]
                for schema in schemas_in_catalog
                if username in schema["full_name"]
            ]
        )
    return schemas


def get_tables_for_schema(api_client: ApiClient, full_name: str) -> list[str]:
    """Find full name of all tables in a schema."""
    catalog, schema = full_name.split(".")
    return [tbl["full_name"] for tbl in api_client.get_tables(catalog, schema)]


def delete_schema(api_client: ApiClient, full_name: str) -> None:
    """Delete a schema including all tables in it."""
    if tables := get_tables_for_schema(api_client, full_name):
        for table in tables:
            logger.info(f"Deleting {table}")
            api_client.delete_table(table)
    logger.info(f"Deleting schema={full_name}")
    api_client.delete_schema(full_name)
