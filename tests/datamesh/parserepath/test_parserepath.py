from brickops.datamesh.parserepath.parse import parsepath

DEFAULT_REGEXP = r"/shared/monorepo/orgs/(?P<org>[^/]+)/domains/(?P<domain>[^/]+)/projects/(?P<project>[^/]+)/(?P<activity>[^/]+)/(?P<flowtype>[^/]+)/(?P<flow>[^/]+)"


def test_parsepath_with_valid_path() -> None:
    path = "/shared/monorepo/orgs/acme/domains/analytics/projects/sales/Flow/prep/load_data"
    result = parsepath(path, DEFAULT_REGEXP)
    assert result == {
        "org": "acme",
        "domain": "analytics",
        "project": "sales",
        "activity": "Flow",
        "flowtype": "prep",
        "flow": "load_data",
    }


def test_parsepath_invalid_path() -> None:
    path = "/some/other/path/that/does/not/match"
    result = parsepath(path, DEFAULT_REGEXP)
    assert result is None


def test_parsepath_no_config() -> None:
    result = parsepath(
        "/shared/monorepo/orgs/acme/domains/analytics/projects/sales/Flow/prep/load_data",
        None,
    )
    assert result is None


def test_parsepath_invalid_regex() -> None:
    result = parsepath("/any/path", "(")
    assert result is None
