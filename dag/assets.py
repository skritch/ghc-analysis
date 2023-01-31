
from dagster import asset, AssetKey
from dagster_dbt import load_assets_from_dbt_project

from .config import DBT_PROFILES, DBT_PROJECT_PATH, DATA_PATH
from scripts import download_opendata



dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES,
    node_info_to_asset_key=lambda node_info: AssetKey([node_info["name"]]),
    # io_manager_key=
)


@asset(group_name="nypd")
def nypd_precinct_addresses() -> None:
    """
    Locations come from https://www.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page
    """
    raise NotImplementedError


@asset(non_argument_deps={"nypd_precinct_addresses"}, group_name="nypd")
def nypd_precinct_locations() -> None:
    """
      
      geocoded with Here API via `geocode_csv.py` script, then hand-editing 3 lat/lons that appear 
      way out in Long Island.

      (https://docs.google.com/spreadsheets/d/13SjM52D9yIXMEczNLoHEYrfPc0qrf8cOuunrd9Ye4QI/edit#gid=0)
    """
    raise NotImplementedError

    
@asset(group_name="nypd")
def nypd_precinct_geometries_csv() -> None:
    """
      Geometries are from https://data.cityofnewyork.us/Public-Safety/Police-Precincts/78dh-3ptz 
      loaded via `load_geo_data.sh` script.
    """
    raise NotImplementedError


@asset(non_argument_deps={"nypd_precinct_geometries_csv"}, group_name="nypd")
def nypd_precinct_geometries() -> None:
    """
      loaded via `load_geo_data.sh` script.
    """
    raise NotImplementedError
  
opendata_shapefiles = [
    asset(name=d + '_shapefile', group_name='districts')(lambda: download_opendata.dl_extract_shapefile(d, url))
    for d, url in download_opendata.SPATIAL_DATASETS
]
opendata_csvs = [
    asset(name=d + '_csv', group_name='districts')(lambda: download_opendata.dl_csv(d, url))
    for d, url in download_opendata.CSV_DATASETS
]
