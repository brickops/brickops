from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from brickops.databricks import api
from brickops.databricks.context import DbContext, current_env, get_context
from brickops.dataops.deploy.pipeline.buildconfig import build_pipeline_config
from brickops.dataops.deploy.readconfig import read_config_yaml
from brickops.dataops.deploy.repo import git_source

if TYPE_CHECKING:
    from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
        PipelineConfig,
    )


def autopipeline(
    cfgyaml: str = "deployment.yml",
    env: str | None = None,
) -> dict[str, Any]:
    """Deploy a pipeline defined in ./deployment.yml.

    Pipeline naming and the rest of the configuration is derived from the environment.
    """
    db_context = get_context()

    if not env:
        env = current_env(db_context)

    if env not in ("test", "dev", "prod"):
        msg = f"env must be 'test', 'dev' or 'prod', not {env}"
        raise ValueError(msg)

    cfg = read_config_yaml(cfgyaml)
    cfg["git_source"] = git_source(db_context)
    pipeline_config = build_pipeline_config(
        cfg=cfg,
        env=env,
        db_context=db_context,
    )
    logging.info(
        "\npipeline_config:\n"
        + json.dumps(pipeline_config.export_dict(), sort_keys=True, indent=4)
    )

    response = create_or_update_pipeline(db_context, pipeline_config)

    logging.info("Pipeline deploy finished.")
    return {"pipeline_name": pipeline_config.name, "response": response}


def create_or_update_pipeline(
    db_context: DbContext, pipeline_config: PipelineConfig
) -> dict[str, Any]:
    api_client = api.ApiClient(db_context.api_url, db_context.api_token)
    if pipeline := api_client.get_pipeline_by_name(pipeline_name=pipeline_config.name):
        return api_client.update_pipeline(
            pipeline_id=pipeline["pipeline_id"],
            pipeline_name=pipeline_config.name,
            pipeline_config=pipeline_config.export_dict(),
        )

    return api_client.create_pipeline(
        pipeline_name=pipeline_config.name,
        pipeline_config=pipeline_config.export_dict(),
    )
