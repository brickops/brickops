from typing import Any

import pytest_mock

from brickops.databricks.api import ApiClientError
from brickops.databricks.context import DbContext
from brickops.dataops.deploy.repo import git_source

REPO = {
    "url": "https://github.com/x/y",
    "provider": "gitHub",
    "branch": "molde",
    "head_commit_id": "98db7c4abc",
    "path": "/Users/u@x.no/course",
}


def _ctx() -> DbContext:
    return DbContext(
        api_url="https://host",
        api_token="t",  # noqa: S106
        notebook_path="/Users/u@x.no/course/flows/prep/nb",
        username="u@x.no",
    )


def _patch_status(mocker: pytest_mock.MockerFixture, statuses: dict[str, Any]) -> None:
    def status(_self: Any, path: str) -> dict[str, Any]:
        if path not in statuses:
            raise ApiClientError("nf")
        return statuses[path]

    mocker.patch("brickops.databricks.api.ApiClient.get_workspace_status", status)


def test_direct_lookup_finds_repo_missing_from_listing(
    mocker: pytest_mock.MockerFixture,
) -> None:
    _patch_status(
        mocker,
        {
            "/Users/u@x.no/course/flows/prep": {"object_type": "DIRECTORY"},
            "/Users/u@x.no/course/flows": {"object_type": "DIRECTORY"},
            "/Users/u@x.no/course": {"object_type": "REPO", "object_id": 7},
        },
    )
    mocker.patch("brickops.databricks.api.ApiClient.get_repo", return_value=REPO)
    listing = mocker.patch(
        "brickops.databricks.api.ApiClient.get_repos", return_value=[]
    )
    result = git_source(_ctx())
    assert result["git_branch"] == "molde"
    assert result["git_commit"] == "98db7c4abc"
    listing.assert_not_called()


def test_is_git_folder_flag_is_recognised(mocker: pytest_mock.MockerFixture) -> None:
    _patch_status(
        mocker,
        {
            "/Users/u@x.no/course": {
                "object_type": "DIRECTORY",
                "directory_info": {"is_git_folder": True},
                "object_id": 7,
            }
        },
    )
    mocker.patch("brickops.databricks.api.ApiClient.get_repo", return_value=REPO)
    assert git_source(_ctx())["git_branch"] == "molde"


def test_listing_fallback_uses_longest_segment_match(
    mocker: pytest_mock.MockerFixture,
) -> None:
    _patch_status(mocker, {})
    sibling = {**REPO, "path": "/Users/u@x.no/cou", "branch": "wrong"}
    mocker.patch(
        "brickops.databricks.api.ApiClient.get_repos", return_value=[sibling, REPO]
    )
    assert git_source(_ctx())["git_branch"] == "molde"


def test_nothing_found_returns_empty(mocker: pytest_mock.MockerFixture) -> None:
    _patch_status(mocker, {})
    mocker.patch("brickops.databricks.api.ApiClient.get_repos", return_value=[])
    assert git_source(_ctx()) == {}


def test_no_api_url_returns_empty() -> None:
    ctx = _ctx()
    ctx.api_url = ""
    assert git_source(ctx) == {}


def test_nearest_git_root_wins_over_outer(mocker: pytest_mock.MockerFixture) -> None:
    _patch_status(
        mocker,
        {
            "/Users/u@x.no/course/flows": {"object_type": "REPO", "object_id": 2},
            "/Users/u@x.no/course": {"object_type": "REPO", "object_id": 1},
        },
    )
    get_repo = mocker.patch(
        "brickops.databricks.api.ApiClient.get_repo", return_value=REPO
    )
    git_source(_ctx())
    get_repo.assert_called_once_with("2")


def test_api_error_on_get_repo_returns_empty(
    mocker: pytest_mock.MockerFixture,
) -> None:
    _patch_status(
        mocker, {"/Users/u@x.no/course": {"object_type": "REPO", "object_id": 1}}
    )
    mocker.patch(
        "brickops.databricks.api.ApiClient.get_repo", side_effect=ApiClientError("boom")
    )
    assert git_source(_ctx()) == {}


def test_listing_fallback_ignores_prefix_only_sibling(
    mocker: pytest_mock.MockerFixture,
) -> None:
    _patch_status(mocker, {})
    sibling = {**REPO, "path": "/Users/u@x.no/course-fork", "branch": "wrong"}
    mocker.patch("brickops.databricks.api.ApiClient.get_repos", return_value=[sibling])
    assert git_source(_ctx()) == {}
