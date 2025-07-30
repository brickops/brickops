from brickops.datamesh.parserepath.parse import parsepath


DEFAULT_REGEXP = r".*/pkg/(?P<pkg>[^>/]+)/(?P<area>[^/]+)/(?P<job>[^/]+)"


def test_parsepath_alt_with_valid_path() -> None:
    path = "/somewhere/pkg/core/logging/myjob"
    result = parsepath(path, DEFAULT_REGEXP)
    assert result == {
        "pkg": "core",
        "area": "logging",
        "job": "myjob",
    }


def test_parsepath_alt_invalid_path() -> None:
    path = "/pkg/onlyonecomponent"
    result = parsepath(path, DEFAULT_REGEXP)
    assert result is None
