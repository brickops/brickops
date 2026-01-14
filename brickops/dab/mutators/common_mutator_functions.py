import re

from databricks.bundles.core import (
    Variable,
    variables,
)


@variables
class Variables:
    """Variables used in the bundle."""

    schema: Variable[str]
    git_branch: Variable[str]


def clean_branch(branch: str) -> str:
    """Strip anything but alphanum from branch name."""
    return re.sub(r"[\W]+", "", branch)
