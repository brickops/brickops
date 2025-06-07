import pytest

from brickops.datamesh.parserepath.parse import parsepath
import brickops.datamesh.cfg as cfg


@pytest.fixture(autouse=True)
def fake_alt_config(monkeypatch):
    # alternate config for tests
    test_regex = r".*/pkg/(?P<pkg>[^>/]+)/(?P<area>[^/]+)/(?P<job>[^/]+)"
    monkeypatch.setattr(
        cfg, "read_config", lambda: {"naming": {"path_regexp": test_regex}}
    )


def test_parsepath_alt_with_valid_path():
    path = "/somewhere/pkg/core/logging/myjob"
    result = parsepath(path)
    assert result == {
        "pkg": "core",
        "area": "logging",
        "job": "myjob",
    }


def test_parsepath_alt_invalid_path():
    path = "/pkg/onlyonecomponent"
    result = parsepath(path)
    assert result is None
