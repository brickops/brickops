import logging
import re
from typing import Any
from dataclasses import dataclass
from brickops.datamesh.cfg import get_config
from brickops.datamesh.parsepath.parse import parsepath

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
    # Determine if a custom path regexp is configured
    naming_root = get_config("naming") or {}
    path_regexp = naming_root.get("path_regexp")
    if path_regexp:
        # Use configurable parser first, then fallback if no match
        from brickops.datamesh.parserepath.parse import parsepath as _parse_by_regex

        parsed_mapping = _parse_by_regex(path)
        if not parsed_mapping:
            logger.debug("Config regex did not match, falling back to default parser")
            parsed_obj = parsepath(path)
            if not parsed_obj:
                return ""
            parsed_mapping = vars(parsed_obj)
    else:
        # Fallback to default parser
        parsed_obj = parsepath(path)
        if not parsed_obj:
            return ""
        parsed_mapping = vars(parsed_obj)
    # Compose the name using dynamic mapping
    naming_config = _get_naming_config(resource=resource, env=pipeline_context.env)
    return _compose_name(
        naming_config=naming_config,
        parsed_mapping=parsed_mapping,
        pipeline_context=pipeline_context,
        resource=resource,
        resource_name=resource_name,
    )


def _compose_name(
    naming_config: str,
    parsed_mapping: dict[str, str],
    pipeline_context: PipelineContext,
    resource: str,
    resource_name: str | None,
) -> str:
    """Compose the name based on the provided naming_config and parsed path mapping."""
    # Start with captured path components
    format_dict: dict[str, Any] = dict(parsed_mapping)
    # Add context variables
    format_dict.update(
        {
            "env": pipeline_context.env,
            "username": pipeline_context.username,
            "gitbranch": pipeline_context.gitbranch,
            "gitshortref": pipeline_context.gitshortref,
        }
    )
    # Include resource name under its placeholder key
    format_dict[resource] = resource_name or ""
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
