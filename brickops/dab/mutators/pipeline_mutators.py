# type: ignore[reportAttributeAccessIssue] known issue due to type structure of databricks variable classes
import logging
from dataclasses import replace
from databricks.bundles.core import (
    Bundle,
    # pipeline_mutator,
)
from databricks.bundles.pipelines import Pipeline

from brickops.databricks.context import get_context_from_params
from brickops.datamesh import naming

from .common_mutator_functions import clean_branch  # , Variables
from .gitinfo import get_git_info
from .mutatorcontext import username_from_path, bundlepath

logger = logging.getLogger(__name__)


def _username(pipeline: Pipeline) -> str:
    return username_from_path(pipeline.configuration["bundle.sourcePath"])


# Don't use decorator here, since it is more pedagogic to use it in mutators.py in the DAB
# @pipeline_mutator
def brickops_pipeline_params(bundle: Bundle, pipeline: Pipeline) -> Pipeline:
    """Adjust schema and pipeline name to brickops naming scheme."""
    logger.debug("bundle: %s", bundle)
    logger.debug("pipeline: %s", pipeline)
    bundle_path = bundlepath()
    username = _username(pipeline)
    git_info = get_git_info()
    logger.debug("git_info parsed from first .git dir found: %s", git_info)
    db_context = get_context_from_params(bundle_path, username, git_info)
    git_branch = git_info["branch"]
    clean_git_branch = clean_branch(git_branch)

    def _adjust_pipeline_name(bundle: Bundle, pipeline: Pipeline) -> None:
        name = naming.pipelinename(db_context, bundle.target)
        logger.debug("pipeline name: %s", name)
        pipeline.name = name

    def _adjust_schema_name(bundle: Bundle, pipeline: Pipeline) -> None:
        dbname = naming.dbname(
            db=pipeline.schema,
            db_context=db_context,
            cat="",  # catalog will be determined by naming config
            prepend_cat=False,
            target=bundle.target,
        )
        logger.debug("schema name: %s", dbname)
        pipeline.schema = dbname

    def _add_pipeline_confs(pipeline: Pipeline) -> None:
        """Use replace, as shown in example:
        https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands

        Not sure if needed. Maybe needed since the dataclass might be frozen.
        """
        configuration = pipeline.configuration or {}
        configuration["brickops.clean_git_branch"] = clean_git_branch
        for key in git_info.keys():
            if key == "repo_root":
                continue
            configuration[f"brickops.git_{key}"] = git_info[key]
        # configuration["git_branch"] = "${{bundle.git.branch}}"
        logger.debug("pipeline configuration: %s", configuration)
        replace(pipeline, configuration=configuration)

    _adjust_pipeline_name(bundle, pipeline)
    _adjust_schema_name(bundle, pipeline)
    _add_pipeline_confs(pipeline)
    return pipeline
