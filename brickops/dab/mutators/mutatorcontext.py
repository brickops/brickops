import logging
import os


logger = logging.getLogger(__name__)


def bundlepath() -> str:
    """Get the bundle path for the bundle."""
    path = os.getcwd()
    logger.debug("bundle_path parsed from pwd path: %s", path)
    return path


def username_from_path(source_path: str) -> str:
    """Get the username from the bundle source path,
    e.g. /Workspace/Users/foo@example.no/.bundle/trip/dev/files/src"""
    # Didn't work: return "${workspace.current_user.userName}"
    username = source_path.split("/Users/")[1].split("/")[0]
    logger.debug("username parsed from pwd path: %s", username)
    return username
