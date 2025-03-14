from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class JobConfig:
    """Represents a job configuration."""

    name: str
    max_concurrent_runs: int
    email_notifications: dict[str, Any]
    schedule: dict[str, Any] | None
    tags: dict[str, Any]
    tasks: list[dict[str, Any]]
    job_clusters: list[dict[str, Any]]
    parameters: list[dict[str, Any]]
    run_as: dict[str, Any]
    git_source: dict[str, Any]

    def update(self, cfg: dict[str, Any]) -> None:
        """Update the job configuration with the given configuration."""
        for key, value in cfg.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def dict(self) -> dict[str, str]:
        # we skip None values to avoid sending them to the API
        return asdict(
            self, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}
        )


def defaultconfig() -> JobConfig:
    return JobConfig(
        name="",
        max_concurrent_runs=1,
        email_notifications={},
        schedule=None,
        tags={},
        tasks=[],
        job_clusters=[],
        parameters=[],
        run_as={},
        git_source={},
    )
