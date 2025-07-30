import logging
import re
from typing import Dict, Optional


logger = logging.getLogger(__name__)


def parsepath(path: str, regex: str | None = None) -> Optional[Dict[str, str]]:
    """Parse a path based on a user-defined regexp in config under naming.path_regexp.

    Returns a dict of named groups if matched, else None.
    """
    # if caller supplied a regex, use it first
    if not regex:
        logger.debug("No naming.path_regexp defined in config.")
        return None
    try:
        pattern = re.compile(regex)
        match = pattern.match(path)
    except re.error as err:
        logger.error(f"Invalid path_regexp pattern: {err}")
        return None
    if not match:
        logger.debug(f"Path did not match configurable regex: {regex}")
        return None
    return match.groupdict()
