from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class PipelineConfig:
    """Represents a pipeline configuration."""

    name: str
    edition: str
    catalog: str | None
    schema: str | None
    data_sampling: bool
    continuous: bool
    channel: str | None
    photon: bool
    pipeline_type: str
    libraries: list[dict[str, dict[str, str]]]
    serverless: bool
    development: bool | None
    tags: dict[str, Any]
    parameters: list[dict[str, Any]]
    pipeline_tasks: list[dict[str, Any]]
    schedule: dict[str, Any] | None
    policy_name: str
    run_as: dict[str, Any] | None
    git_source: dict[str, Any]

    def update(self, cfg: dict[str, Any]) -> None:
        """Update the pipeline configuration with the given configuration."""
        for key, value in cfg.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def export_dict(self) -> dict[str, str]:
        """Export the pipeline configuration to a dictionary for the API call.
        We avoid intermediate parameters like pipeline_tasks, which
        are converted to another format.
        We also skip None values to avoid sending them to the API."""
        # policy_name possible causes CHANGES_UC_PIPELINE_TO_HMS_NOT_ALLOWED on update
        INTERMEDIATE_PARAMS = {"pipeline_tasks", "git_source", "run_as", "policy_name"}
        return asdict(
            self,
            dict_factory=lambda x: {
                k: v for (k, v) in x if v is not None and k not in INTERMEDIATE_PARAMS
            },
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
