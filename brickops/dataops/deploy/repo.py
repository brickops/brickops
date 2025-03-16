from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from brickops.databricks import api

if TYPE_CHECKING:
    from brickops.databricks.context import DbContext

logger = logging.getLogger(__name__)


def git_source(db_context: DbContext) -> dict[str, Any]:
    """Get git source information for a repo."""
    if not db_context.api_url:
        return {}

    try:
        api_client = api.ApiClient(db_context.api_url, db_context.api_token)
        repos = api_client.get_repos()
        repo = next(r for r in repos if db_context.notebook_path.startswith(r["path"]))
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
