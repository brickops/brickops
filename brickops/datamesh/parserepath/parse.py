import logging
import re
from typing import Dict, Optional

import brickops.datamesh.cfg as cfg

logger = logging.getLogger(__name__)


def parsepath(path: str) -> Optional[Dict[str, str]]:
    """Parse a path based on a user-defined regexp in config under naming.path_regexp.

    Returns a dict of named groups if matched, else None.
    """
    config = cfg.read_config()
    if not config:
        logger.debug("No config found for configurable path parsing.")
        return None
    naming = config.get("naming", {})
    regex = naming.get("path_regexp")
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
