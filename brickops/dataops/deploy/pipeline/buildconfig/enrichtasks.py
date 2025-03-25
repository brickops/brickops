import os.path

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import PipelineConfig
from brickops.datamesh.naming import extract_catname_from_path, escape_sql_name, dbname


def enrich_tasks(
    pipeline_config: PipelineConfig, db_context: DbContext, env: str
) -> PipelineConfig:
    new_pipeline = pipeline_config.pipeline_tasks[0]
    pipeline_config.pipeline_tasks = None
    # Set target catalog
    cat = escape_sql_name(extract_catname_from_path(db_context.notebook_path))
    db = new_pipeline.pop("db")
    pipeline_config.catalog = cat
    # Set target database/schema
    pipeline_key = new_pipeline["pipeline_key"]
    pipeline_config.schema = dbname(
        cat=cat, db=db, db_context=db_context, prepend_cat=False, env=env
    )
    # Set development mode for all envs except prod
    pipeline_config.development = env != "prod"
    # For now, dlt does not support gitrefs, so we must use absolute path
    # chip off notebook name, and return its folder
    base_nb_path = os.path.dirname(db_context.notebook_path)
    pipeline_config.libraries = [
        {
            "notebook": {
                "path": f"{base_nb_path}/{pipeline_key}",
            }
        }
    ]
    return pipeline_config
