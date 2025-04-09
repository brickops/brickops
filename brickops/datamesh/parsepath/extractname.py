import logging
import re
from typing import Any
from dataclasses import dataclass
from brickops.datamesh.cfg import get_config
from brickops.datamesh.parsepath.parse import parsepath, ParsedPath

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    username: str
    gitbranch: str
    gitshortref: str
    env: str


def extract_name_from_path(
    *,
    path: str,
    resource: str,
    pipeline_context: PipelineContext,
    resource_name: str | None = None,
) -> str:
    naming_config = _get_naming_config(resource=resource, env=pipeline_context.env)
    parsed_path = parsepath(path)
    if not parsed_path:
        return ""
    return _compose_name(
        naming_config=naming_config,
        parsed_path=parsed_path,
        pipeline_context=pipeline_context,
        resource=resource,
        resource_name=resource_name,
    )


def _compose_name(
    naming_config: str,
    parsed_path: ParsedPath,
    pipeline_context: PipelineContext,
    resource: str,
    resource_name: str | None,
) -> str:
    """Compose the name based on the provided naming_config and parsed path.

    Example naming_config string: "{env}_{username}_{gitbranch}_{gitref}_{db}"
    """
    # Create a dictionary with all possible variables
    format_dict = {
        "org": parsed_path.org if parsed_path.org else "",
        "domain": parsed_path.domain,
        "project": parsed_path.project,
        "activity": parsed_path.activity if parsed_path.activity else "",
        "flowtype": parsed_path.flowtype,
        "flow": parsed_path.flow,
        "env": pipeline_context.env,
        "username": pipeline_context.username,
        "gitbranch": pipeline_context.gitbranch,
        "gitshortref": pipeline_context.gitshortref,
        resource: resource_name,
    }
    logger.info("extractname.py:" + repr(61) + ":naming_config:" + repr(naming_config))
    # Replace the variables in the template with actual values
    return naming_config.format(**format_dict)


def _get_naming_config(resource: str, env: str) -> str:
    """Get the naming configuration for the given resource."""
    config = _get_nested_config("naming", resource)
    if not config:
        config = DEFAULT_CONFIGS[resource]
    if env in config:
        config_str = config[env]
    else:  # Use default 'other' config if env not specified
        config_str = config["other"]
    _validate_naming_config(config_str)
    return config_str


def _validate_naming_config(config: str) -> None:
    """Validate that config string only contains alphanum, underscore, hyphen
    and curly brackets.
    E.g. '{env}_{username}_{branch}_{gitshortref}_{db}'"""
    if not re.match(r"^[\w\{\}_\-]+$", config):
        raise ValueError(
            f"Invalid naming config '{config}'. Only alphanumeric characters, underscores, hyphens, and curly brackets are allowed."
        )


def _get_nested_config(key: str, resource: str) -> Any | None:
    """Get a nested configuration value from yaml config or default."""
    config = get_config(key)
    if config is None:
        return None
    return config.get(resource, None)


DEFAULT_CONFIGS = {
    "job": {
        "prod": "{domain}_{project}_{env}",
        "other": "{domain}_{project}_{env}_{username}_{gitbranch}_{gitshortref}",
    },
    "pipeline": {
        "prod": "{domain}_{project}_{env}_dlt",
        "other": "{domain}_{project}_{env}_{username}_{gitbranch}_{gitshortref}_dlt",
    },
    "catalog": {
        "prod": "{domain}",
        "other": "{domain}",
    },
    "db": {
        "prod": "{db}",
        "other": "{env}_{username}_{gitbranch}_{gitshortref}_{db}",
    },
}
