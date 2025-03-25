from pathlib import Path

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.job.buildconfig.clusters import (
    add_clusters,
    lookup_cluster_id,
)
from brickops.dataops.deploy.job.buildconfig.job_config import JobConfig
from brickops.dataops.deploy.nbpath import nbrelfolder


def enrich_tasks(job_config: JobConfig, db_context: DbContext) -> JobConfig:
    tasks = job_config.tasks
    used_clusters = {}
    for task in tasks:
        base_path = nbrelfolder(
            db_context, root_folder=job_config.git_source["git_path"]
        )
        if "notebook_task" not in task:
            task_key = task["task_key"]
            task["notebook_task"] = {
                "notebook_path": str(Path(base_path) / task_key),
                "source": "GIT",
            }
        else:
            nb_task = task["notebook_task"]
            if "source" not in nb_task:
                nb_task["source"] = "GIT"
                nb_task["notebook_path"] = str(
                    Path(base_path) / Path(nb_task["notebook_path"])
                )
                continue

        # Either a job_cluster_key, an existing_cluster_id, or serverless
        if task.get("serverless", False):
            # Make sure a job_cluster_key or existing_cluster_name is not defined
            # to avoid ambiguous configs
            if "job_cluster_key" in task or "existing_cluster_name" in task:
                msg = """
                The task is specified both with `serverless: true` and a
                job_cluster_key or existing_cluster_name.
                Either remove the cluster specification or the serverless statement.
                """
                raise ValueError(msg)
            # If a task does not contain an existing_cluster_id or
            # a job_cluster_key it will automatically use serverless
            # compute.
            del task["serverless"]
            continue

        if "job_cluster_key" in task:
            env_cluster = task["job_cluster_key"]
            used_clusters[task["job_cluster_key"]] = {"env_cluster_key": env_cluster}
            task["job_cluster_key"] = env_cluster
        elif "existing_cluster_name" in task:
            # Ensure we have a cluster reference
            existing_cluster_name = task.pop("existing_cluster_name")
            task["existing_cluster_id"] = lookup_cluster_id(
                db_context=db_context, cluster_name=existing_cluster_name
            )
        elif "existing_cluster_id" not in task:
            msg = "No cluster references found"
            raise ValueError(msg)

    # Get list of clusters used by tasks
    return add_clusters(job_config=job_config, used_clusters=used_clusters)
