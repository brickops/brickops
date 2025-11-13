import logging
from typing import Any
from brickops.databricks.context import DbContext
from brickops.databricks.username import get_username
from brickops.datamesh.naming import pipelinename
from brickops.dataops.deploy.pipeline.buildconfig.enrichtasks import enrich_tasks
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
    PipelineConfig,
    defaultconfig,
)
from brickops.gitutils import clean_branch, commit_shortref


logger = logging.getLogger(__name__)


def depname(*, db_context: DbContext, target: str, git_src: dict[str, Any]) -> str:
    """Compose deployment name from target and git config."""
    if target == "prod":
        return "prod"
    uname = get_username(db_context)
    branch = clean_branch(git_src["git_branch"])
    short_ref = commit_shortref(git_src["git_commit"])
    return f"{target}_{uname}_{branch}_{short_ref}"


def build_pipeline_config(
    cfg: dict[str, Any],
    target: str,
    db_context: DbContext,
) -> PipelineConfig:
    """Combine custom parameters with default parameters, and default cluster config."""
    full_cfg = defaultconfig()
    full_cfg.update(cfg)
    full_cfg.name = pipelinename(db_context, target=target)
    dep_name = depname(
        db_context=db_context, target=target, git_src=full_cfg.git_source
    )
    tags = _tags(cfg=cfg, depname=dep_name, target=target)
    full_cfg.tags = tags
    full_cfg.parameters.extend(build_context_parameters(target, tags))
    logger.info("full_cfg:" + repr(full_cfg))
    full_cfg = enrich_tasks(
        pipeline_config=full_cfg, db_context=db_context, target=target
    )
    return full_cfg


def build_context_parameters(target: str, tags: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a list of parameters containing the target and git info."""
    return [
        {
            "name": "target",
            "default": target,
        },
        {
            "name": "git_url",
            "default": tags["git_url"],
        },
        {
            "name": "git_branch",
            "default": tags["git_branch"],
        },
        {
            "name": "git_commit",
            "default": tags["git_commit"],
        },
    ]


def _tags(*, cfg: dict[str, Any], depname: str, target: str) -> dict[str, Any]:
    return {
        "deployment": depname,
        "git_url": cfg["git_source"]["git_url"],
        "git_branch": cfg["git_source"]["git_branch"],
        "git_commit": cfg["git_source"]["git_commit"],
        "target": target,
    }
