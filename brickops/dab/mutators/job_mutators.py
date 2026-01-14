# type: ignore[reportAttributeAccessIssue] known issue due to type structure of databricks variable classes
import logging

from databricks.bundles.core import (
    Bundle,
    # job_mutator,
)
from databricks.bundles.jobs import (
    Job,
    PauseStatus,
    PerformanceTarget,
)

from brickops.databricks.context import get_context_from_params
from brickops.datamesh import naming
from .common_mutator_functions import clean_branch
from .mutatorcontext import username_from_path, bundlepath
from .gitinfo import get_git_info


logger = logging.getLogger(__name__)


def _username(path: str) -> str:
    return username_from_path(path)


# @job_mutator
def brickops_job_params(
    bundle: Bundle,
    job: Job,
) -> Job:  # noqa: C901
    logger.debug("bundle: %s", bundle)
    logger.debug("job: %s", job)
    bundle_path = bundlepath()
    username = _username(bundle_path)
    git_info = get_git_info()
    logger.debug("git_info parsed from first .git dir found: %s", git_info)
    db_context = get_context_from_params(bundle_path, username, git_info)
    clean_git_branch = clean_branch(git_info["branch"])
    target = bundle.target

    # def _schema_name(target: str, job: Job) -> None:
    #     dbname = naming.dbname(
    #         db=job.schema,
    #         db_context=db_context,
    #         cat="",  # catalog will be determined by naming config
    #         prepend_cat=False,
    #         target=target,
    #     )
    #     logger.debug("schema name: %s", dbname)
    #     return dbname

    def _adjust_job_name(target: str, job: Job) -> None:
        name = naming.jobname(db_context, target)
        logger.debug("job name: %s", name)
        job.name = name

    def _adjust_parameter(job: Job, name: str, value: str) -> None:
        current_params = job.parameters
        if type(current_params) is list:
            if current_param := next(
                (param for param in current_params if param.name == name), None
            ):
                current_param.default = value

    def _adjust_parameters(target: str, job: Job, git_info: dict) -> None:
        """Adjust job parameters to support brickops naming."""
        _adjust_parameter(job, "source_path", bundle_path)
        _adjust_parameter(job, "target", target)
        #         _adjust_parameter(job, "schema", _schema_name(target, job))
        _adjust_parameter(job, "clean_git_branch", clean_git_branch)
        for key in git_info.keys():
            if key == "repo_root":
                continue
            _adjust_parameter(job, "git_{key}", git_info[key])

    def _adjust_non_prod_perf_target(job: Job) -> None:
        job.performance_target = PerformanceTarget.PERFORMANCE_OPTIMIZED

    def _adjust_non_prod_triggers(job: Job):
        if job.trigger:
            job.trigger.pause_status = PauseStatus.PAUSED

    def _adjust_non_prod_schedules(job: Job) -> None:
        if job.schedule is not None:
            job.schedule.pause_status = PauseStatus.PAUSED

    _adjust_job_name(target, job)
    _adjust_parameters(target, job, git_info)
    if bundle.target != "prod":
        _adjust_non_prod_perf_target(job)
        _adjust_non_prod_schedules(job)
        _adjust_non_prod_triggers(job)

    return job
