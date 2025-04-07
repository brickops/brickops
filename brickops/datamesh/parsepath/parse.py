import logging
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


def parsepath(path: str) -> ParsedPath | None:
    """Parse path to extract org, domain, project, and flow."""
    has_org = "/org/" in path
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
