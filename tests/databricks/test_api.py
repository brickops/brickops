from typing import Any

import pytest
import requests

from brickops.databricks.api import ApiClient, ApiClientError


def test_get_jobs_with_no_results(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    requests_mock.get("https://test.com/api/2.2/jobs/list", json={"jobs": []})
    assert client.get_jobs() == []


def test_get_jobs_with_result(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    requests_mock.get("https://test.com/api/2.2/jobs/list", json={"jobs": [{"id": 1}]})
    assert client.get_jobs() == [{"id": 1}]


def test_get_jobs_with_paginated_result(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    requests_mock.get(
        "https://test.com/api/2.2/jobs/list",
        json={"jobs": [{"id": 1}], "next_page_token": "token"},
    )
    requests_mock.get(
        "https://test.com/api/2.2/jobs/list?page_token=token",
        json={"jobs": [{"id": 2}], "next_page_token": None},
    )
    assert client.get_jobs() == [{"id": 1}, {"id": 2}]


def test_get_jobs_with_requests_exception(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    requests_mock.get(
        "https://test.com/api/2.2/jobs/list", exc=requests.exceptions.RequestException
    )
    with pytest.raises(ApiClientError) as exc:
        client.get_jobs()

    assert exc.value.message == "Api error while making GET call:"


def test_delete_job_with_requests_exception(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    requests_mock.post(
        "https://test.com/api/2.1/jobs/delete", exc=requests.exceptions.RequestException
    )
    with pytest.raises(ApiClientError) as exc:
        client.delete_job(job_id="1")

    assert exc.value.message == "Api error while making POST call:"


def test_delete_schema_with_requests_exception(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    schema_name = "test.schema.table"
    requests_mock.delete(
        f"https://test.com/api/2.1/unity-catalog/schemas/{schema_name}",
        exc=requests.exceptions.RequestException,
    )
    with pytest.raises(ApiClientError) as exc:
        client.delete_schema(full_name=schema_name)

    assert exc.value.message == "Api error while making DELETE call:"


def test_delete_table_with_requests_exception(requests_mock: Any) -> None:  # noqa: ANN401
    client = ApiClient("https://test.com", "test_token")
    schema_name = "test.schema.table"
    requests_mock.delete(
        f"https://test.com/api/2.1/unity-catalog/tables/{schema_name}",
        exc=requests.exceptions.RequestException,
    )
    with pytest.raises(ApiClientError) as exc:
        client.delete_table(full_name=schema_name)

    assert exc.value.message == "Api error while making DELETE call:"
