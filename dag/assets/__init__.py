
from dagster import AssetKey
from dagster_dbt import load_assets_from_dbt_project

from dag.config import DBT_PROFILES_DIR, DBT_PROJECT_PATH

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES_DIR,
    use_build_command=True,  # needed to run seeds
    node_info_to_asset_key=lambda node_info: AssetKey([node_info["name"]]),
)
