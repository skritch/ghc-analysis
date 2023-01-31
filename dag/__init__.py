
import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import dbt_cli_resource
from dagster_postgres import DagsterPostgresStorage
from dagster import Definitions, load_assets_from_modules, fs_io_manager

from .config import DBT_PROFILES, DBT_PROJECT_PATH
from . import assets

resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    # TODO: add a postgres-pandas iomanager
}

defs = Definitions(assets=load_assets_from_modules([assets]), resources=resources)

