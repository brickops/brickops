"""Fixtures for DAB tests."""

import sys
from unittest.mock import MagicMock

# Mock databricks.bundles modules at import time (before any imports)
sys.modules["databricks.bundles"] = MagicMock()
sys.modules["databricks.bundles.core"] = MagicMock()
sys.modules["databricks.bundles.pipelines"] = MagicMock()
sys.modules["databricks.bundles.jobs"] = MagicMock()
