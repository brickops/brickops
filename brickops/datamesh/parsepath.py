import logging
import os
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ParsedPath:
    flow: str
    project: str
    domain: str
    org: Optional[str] = None


def extract_catname_from_path(path: str) -> str:
    """Derive catalog name from repo data mesh structure.

    By default we simply use domain as base catalog name.
    If an env var BRICKOPS_MESH_CATALOG_LEVELS is defined, then the catalog name
    is composed according to the elements, e.g. f"{org}_{domain}_{project}"
    """
    levels = _mesh_catalog_levels()
    parsed_path = _parse_catalog_path(path)
    return _extract_name_from_path(levels=levels, parsed_path=parsed_path)


def extract_jobprefix_from_path(path: str) -> str:
    """Derive catalog name from repo data mesh structure.

    By default we simply use domain as base catalog name.
    If an env var BRICKOPS_MESH_JOB_LEVELS is defined, then the catalog name
    is composed according to the elements, e.g. f"{org}_{domain}_{project}"
    """
    levels = _mesh_jobprefix_levels()
    parsed_path = _parse_jobprefix_path(path)
    return _extract_name_from_path(levels=levels, parsed_path=parsed_path)


def _extract_name_from_path(levels: list[str], parsed_path: ParsedPath | None) -> str:
    """Derive name from repo data mesh structure.

    By default we simply use domain as base catalog name.
    If an env var like BRICKOPS_MESH_CATALOG_LEVELS is defined, then the name
    is composed according to the elements, e.g. f"{org}_{domain}_{project}"
    """
    if parsed_path:
        vals = [getattr(parsed_path, level) for level in levels]
        return ("_").join(vals)
    return ""


def _parse_catalog_path(path: str) -> ParsedPath | None:
    has_org = _has_catalog_org()
    return _parse_path(path, has_org=has_org)


def _parse_jobprefix_path(path: str) -> ParsedPath | None:
    has_org = _has_jobprefix_org()
    return _parse_path(path, has_org=has_org)


def _parse_path(path: str, has_org: bool) -> ParsedPath | None:
    """Parse path to extract org, domain, project, and flow."""
    if has_org:  # Include org section if required
        rexp = r".*\/org/([^/]+)\/domains/([^/]+)\/projects\/([^/]+)\/(?:flows|explore(\/ml|\/prep)?)\/([^/]+)\/.+"
    else:
        rexp = r".*\/domains\/([^/]+)\/projects\/([^/]+)\/(?:flows|explore(\/ml|\/prep)?)\/([^/]+)\/.+"
    re_ret = re.search(
        rexp,
        path,
        re.IGNORECASE,
    )
    if re_ret is None:
        return None

    expected_levels = 5 if has_org else 4
    if len(re_ret.groups()) < expected_levels:  # noqa: PLR2004
        logger.warning(
            """_parse_catalog_path: unexpected number of groups for full mesh.
            Is the notebook in the correct folder!?"""
        )
        return None

    if has_org:
        return ParsedPath(
            org=re_ret[1],
            domain=re_ret[2],
            project=re_ret[3],
            flow=re_ret[5],
        )
    return ParsedPath(
        domain=re_ret[1],
        project=re_ret[2],
        flow=re_ret[4],
    )


def _mesh_catalog_levels() -> list[str]:
    return _mesh_levels(_env_mesh_catalog_levels())


def _mesh_jobprefix_levels() -> list[str]:
    return _mesh_levels(_env_mesh_jobprefix_levels())


def _mesh_levels(levels_str: str) -> list[str]:
    """Return mesh level conf from env or 'domain' by default.

    Format can be 'org,domain,project'
    or 'domain,project'

    If no org, then we don't look for org in regexp
    """
    levels = levels_str.split(",")
    for level in levels:
        # if level is not alphanum, then we have an error
        if not level.isalnum():
            raise ValueError(f"Invalid mesh level: {level}")
    return levels


def _env_mesh_catalog_levels() -> str:
    return os.environ.get("BRICKOPS_MESH_CATALOG_LEVELS", "domain")


def _env_mesh_jobprefix_levels() -> str:
    return os.environ.get("BRICKOPS_MESH_JOBPREFIX_LEVELS", "domain,project,flow")


def _has_catalog_org() -> bool:
    levels = _mesh_catalog_levels()
    return levels[0] == "org"


def _has_jobprefix_org() -> bool:
    levels = _mesh_jobprefix_levels()
    return levels[0] == "org"
