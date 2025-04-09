import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ParsedPath:
    domain: str
    project: str
    flow: str
    flowtype: str
    org: Optional[str] = None
    activity: Optional[str] = None


def parsepath(path: str) -> ParsedPath | None:
    """Parse path to extract org, domain, project, and flow."""
    empty_parsed_path = ParsedPath(
        domain="",
        project="",
        flowtype="",
        flow="",
    )
    parsed_path = _parsebase(path=path, parsed_path=empty_parsed_path)
    if parsed_path is None:
        return empty_parsed_path

    return _parseflow(path=path, parsed_path=empty_parsed_path)


def _parsebase(*, path: str, parsed_path: ParsedPath) -> ParsedPath | None:
    """Parse base section of path to extract org, domain, project."""
    has_org = "/org/" in path
    if has_org:  # Include org section if required
        rexp = r".*\/org/([^/]+)\/domains/([^/]+)\/projects\/([^/]+)\/.+"
    else:
        rexp = r".*\/domains\/([^/]+)\/projects\/([^/]+)\/.+"
    re_ret = re.search(
        rexp,
        path,
        re.IGNORECASE,
    )
    if re_ret is None:
        logger.info(
            """_parsebase: path regexp not matching, could be valid,
            e.g. for dbname() run outside mesh structure, where mesh names
            (org, domain, project etc) are not used"""
        )
        return None
    if has_org:
        parsed_path.org = re_ret[1]
        parsed_path.domain = re_ret[2]
        parsed_path.project = re_ret[3]
    else:
        parsed_path.domain = re_ret[1]
        parsed_path.project = re_ret[2]
    return parsed_path


def _parseflow(*, path: str, parsed_path: ParsedPath) -> ParsedPath:
    """Parse flow section of path to extract activity, flowtype and flow.
    If only two directory levels are present, assume no activity."""
    rexp = r".*\/domains\/[^/]+\/projects\/[^/]+\/([^/]+)\/([^/]+)\/([^/]+).*"
    re_ret = re.search(
        rexp,
        path,
        re.IGNORECASE,
    )
    if re_ret:
        parsed_path.activity = re_ret[1]  # E.g. Flow or explore
        parsed_path.flowtype = re_ret[2]  # E.g prep or ml
        parsed_path.flow = re_ret[3]  # Name of notebook
        return parsed_path
    # missing a level, so assume no flowtype
    rexp = r".*\/domains\/[^/]+\/projects\/[^/]+\/([^/]+)\/([^/]+).*"
    re_ret = re.search(
        rexp,
        path,
        re.IGNORECASE,
    )
    if re_ret:
        parsed_path.activity = re_ret[1]  # E.g. Flow or explore
        parsed_path.flow = re_ret[2]  # Name of notebook
        return parsed_path
    logger.info("""_parseflow: no matching explore/flow pattern found""")
    return parsed_path
