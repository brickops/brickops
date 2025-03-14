import logging
from typing import Any

from brickops.databricks import api
from brickops.databricks.context import DbContext
from brickops.dataops.deploy.job.buildconfig.job_config import JobConfig

logger = logging.getLogger(__name__)


def add_clusters(job_config: JobConfig, used_clusters: dict[str, Any]) -> JobConfig:
    """Add clusters used by tasks as job_clusters entry in job config."""
    clusters = [
        _cluster(template_key=template_key, key=cluster["env_cluster_key"])
        for template_key, cluster in used_clusters.items()
    ]

    job_config.job_clusters = clusters
    return job_config


def lookup_cluster_id(*, db_context: DbContext, cluster_name: str) -> dict[str, Any]:
    api_client = api.ApiClient(db_context.api_url, db_context.api_token)
    cluster_list = api_client.get_clusters()

    for cluster in cluster_list:
        if cluster["cluster_name"] == cluster_name:
            return cluster["cluster_id"]  # type: ignore [no-any-return]
    msg = f"Cluster {cluster_name} not found"
    raise RuntimeError(msg)


def _cluster(*, template_key: str, key: str) -> dict[str, Any]:
    logger.info(f"template_key: {template_key}, key: {key}")
    templates = cluster_templates()
    cluster = templates[template_key]
    cluster["job_cluster_key"] = key
    return cluster  # type: ignore [no-any-return]


def cluster_templates() -> dict[str, Any]:
    """Templates for different cluster types."""
    # TODO @jesloper: This must be updated
    return {
        "common-job-cluster": {
            "new_cluster": {
                "num_workers": 1,
                "spark_version": "14.3.x-scala2.12",
                "spark_conf": {},
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK_AZURE",
                    "spot_bid_max_price": -1,
                },
                "node_type_id": "Standard_D4ads_v5",
                "ssh_public_keys": [],
                "custom_tags": {},
                "spark_env_vars": {},
                "init_scripts": [],
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
            }
        }
    }
