import pytest

from brickops.datamesh.parserepath.parse import parsepath
import brickops.datamesh.cfg as cfg


@pytest.fixture(autouse=True)
def fake_config(monkeypatch):
    # default config for tests
    test_regex = r"/shared/monorepo/orgs/(?P<org>[^/]+)/domains/(?P<domain>[^/]+)/projects/(?P<project>[^/]+)/(?P<activity>[^/]+)/(?P<flowtype>[^/]+)/(?P<flow>[^/]+)"
    monkeypatch.setattr(
        cfg, "read_config", lambda: {"naming": {"path_regexp": test_regex}}
    )


def test_parsepath_with_valid_path():
    path = "/shared/monorepo/orgs/acme/domains/analytics/projects/sales/Flow/prep/load_data"
    result = parsepath(path)
    assert result == {
        "org": "acme",
        "domain": "analytics",
        "project": "sales",
        "activity": "Flow",
        "flowtype": "prep",
        "flow": "load_data",
    }


def test_parsepath_invalid_path():
    path = "/some/other/path/that/does/not/match"
    result = parsepath(path)
    assert result is None


def test_parsepath_no_config(monkeypatch):
    monkeypatch.setattr(cfg, "read_config", lambda: None)
    result = parsepath(
        "/shared/monorepo/orgs/acme/domains/analytics/projects/sales/Flow/prep/load_data"
    )
    assert result is None


def test_parsepath_invalid_regex(monkeypatch):
    monkeypatch.setattr(cfg, "read_config", lambda: {"naming": {"path_regexp": "("}})
    result = parsepath("/any/path")
    assert result is None
