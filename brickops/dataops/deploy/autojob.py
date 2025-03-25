from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from brickops.databricks import api
from brickops.databricks.context import DbContext, current_env, get_context
from brickops.dataops.deploy.job.buildconfig import build_job_config
from brickops.dataops.deploy.readconfig import read_config_yaml
from brickops.dataops.deploy.repo import git_source

if TYPE_CHECKING:
    from brickops.dataops.deploy.job.buildconfig.job_config import JobConfig

logger = logging.getLogger(__name__)


def autojob(
    cfgyaml: str = "deployment.yml",
    env: str | None = None,
) -> dict[str, Any]:
    """Deploy a job defined in ./deployment.yml.

    Job naming and the rest of the configuration is derived from the environment.
    """
    db_context = get_context()

    if not env:
        env = current_env(db_context)

    if env not in ("test", "dev", "prod"):
        msg = f"env must be 'test', 'dev' or 'prod', not {env}"
        raise ValueError(msg)

    cfg = read_config_yaml(cfgyaml)
    cfg["git_source"] = git_source(db_context)
    job_config = build_job_config(
        cfg=cfg,
        env=env,
        db_context=db_context,
    )
    logger.info(
        "\njob_config:\n" + json.dumps(job_config.dict(), sort_keys=True, indent=4)
    )

    response = create_or_update_job(db_context, job_config)

    logger.info("Job deploy finished.")
    return {"job_name": job_config.name, "response": response}


def create_or_update_job(
    db_context: DbContext, job_config: JobConfig
) -> dict[str, Any]:
    api_client = api.ApiClient(db_context.api_url, db_context.api_token)
    if job := api_client.get_job_by_name(job_name=job_config.name):
        return api_client.update_job(
            job_id=job["job_id"], job_name=job_config.name, job_config=job_config.dict()
        )

    return api_client.create_job(
        job_name=job_config.name,
        job_config=job_config.dict(),
    )
