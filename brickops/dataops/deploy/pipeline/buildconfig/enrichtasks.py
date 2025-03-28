import os.path

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import PipelineConfig
from brickops.datamesh.naming import escape_sql_name, dbname
from brickops.datamesh.parsepath import extract_catname_from_path


def enrich_tasks(
    pipeline_config: PipelineConfig, db_context: DbContext, env: str
) -> PipelineConfig:
    # Set target catalog
    cat = escape_sql_name(extract_catname_from_path(db_context.notebook_path))
    pipeline_config.catalog = cat
    # Set target database/schema
    if not pipeline_config.schema:
        raise ValueError("Schema must be defined in pipeline_config")
    pipeline_config.schema = dbname(
        cat=cat,
        db=pipeline_config.schema,
        db_context=db_context,
        prepend_cat=False,
        env=env,
    )
    # Set development mode for all envs except prod
    pipeline_config.development = env != "prod"
    # For now, dlt does not support gitrefs, so we must use absolute path
    # chip off notebook name, and return its folder
    base_nb_path = os.path.dirname(db_context.notebook_path)
    # Add notebook entries for each task
    for task in pipeline_config.pipeline_tasks:
        pipeline_key = task["pipeline_key"]
        pipeline_config.libraries.append(
            {
                "notebook": {
                    "path": f"{base_nb_path}/{pipeline_key}",
                }
            }
        )
    return pipeline_config
