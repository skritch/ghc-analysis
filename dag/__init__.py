
from dagster import Definitions, load_assets_from_package_module
from dagster_dbt import dbt_cli_resource

from .config import DBT_PROFILES_DIR, DBT_PROJECT_PATH
from . import assets

resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES_DIR,
        },
    ),
    # TODO: add a postgres-pandas iomanager
}


defs = Definitions(
    assets=load_assets_from_package_module(assets), 
    resources=resources
)

