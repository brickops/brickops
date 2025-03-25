from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class PipelineConfig:
    """Represents a pipeline configuration."""

    name: str
    edition: str
    catalog: dict[str, Any] | None
    development: dict[str, Any]
    schema: dict[str, Any] | None
    data_sampling: bool
    continuous: bool
    channel: dict[str, Any] | None
    photon: bool
    pipeline_type: str
    libraries: list[str]
    serverless: bool
    development: list[dict[bool, Any]] | None
    tags: dict[str, Any]
    parameters: list[dict[str, Any]]
    pipeline_tasks: list[dict[str, Any]] | None
    schedule: dict[str, Any] | None
    policy_name: str
    run_as: dict[str, Any] | None
    git_source: dict[str, Any] | None

    def update(self, cfg: dict[str, Any]) -> None:
        """Update the pipeline configuration with the given configuration."""
        for key, value in cfg.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def dict(self) -> dict[str, str]:
        # we skip None values to avoid sending them to the API
        return asdict(
            self, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}
        )


def defaultconfig() -> PipelineConfig:
    return PipelineConfig(
        name="",
        edition="ADVANCED",
        catalog=None,
        development=None,
        schema=None,
        data_sampling=False,
        continuous=False,
        channel="CURRENT",
        photon=True,
        pipeline_type="WORKSPACE",
        libraries=[],
        serverless=True,
        tags={},
        parameters=[],
        pipeline_tasks=[],
        schedule=None,
        policy_name="dlt_default_policy",
        run_as=None,
        git_source={},
    )
