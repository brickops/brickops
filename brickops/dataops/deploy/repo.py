from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from brickops.databricks import api

if TYPE_CHECKING:
    from brickops.databricks.context import DbContext

logger = logging.getLogger(__name__)


def _is_git_root(status: dict[str, Any]) -> bool:
    return bool(
        status.get("object_type") == "REPO"
        or status.get("directory_info", {}).get("is_git_folder")
    )


def _find_repo(api_client: api.ApiClient, notebook_path: str) -> dict[str, Any]:
    """Find the git folder containing the notebook.

    Walks up the notebook path and looks up each parent directly, since the
    repos listing api does not reliably return all git folders.
    """
    parts = notebook_path.strip("/").split("/")
    for depth in range(len(parts) - 1, 0, -1):
        path = "/" + "/".join(parts[:depth])
        try:
            status = api_client.get_workspace_status(path)
        except api.ApiClientError:
            continue
        if _is_git_root(status):
            return api_client.get_repo(str(status["object_id"]))
    raise StopIteration


def _find_repo_in_listing(
    api_client: api.ApiClient, notebook_path: str
) -> dict[str, Any]:
    """Fallback: longest repo path that contains the notebook."""
    matches = [
        r
        for r in api_client.get_repos()
        if notebook_path.startswith(r["path"].rstrip("/") + "/")
    ]
    if not matches:
        raise StopIteration
    return max(matches, key=lambda r: len(r["path"]))


def git_source(db_context: DbContext) -> dict[str, Any]:
    """Get git source information for a repo."""
    if not db_context.api_url:
        return {}

    try:
        api_client = api.ApiClient(db_context.api_url, db_context.api_token)
        try:
            repo = _find_repo(api_client, db_context.notebook_path)
        except StopIteration:
            repo = _find_repo_in_listing(api_client, db_context.notebook_path)
        return {
            "git_url": repo["url"],
            "git_provider": repo["provider"],
            "git_branch": repo.get("branch", ""),
            "git_commit": repo["head_commit_id"],
            "git_path": repo["path"],
        }
    except api.ApiClientError:
        logger.warning("Failed while getting git information from api")
        return {}
    except StopIteration:
        logger.info(
            "Repo does not exists or user does not have access to git information."
        )
        return {}
