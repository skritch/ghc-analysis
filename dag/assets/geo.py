
from typing import NamedTuple
from zipfile import ZipFile

import requests
from dagster import asset
from dag.assets.constants import NYC_GOV_API_URL, OPENDATA_SPATIAL_URL, OPENDATA_URL
from dag.config import (DATA_PATH, SCRIPTS_DIR)
from dag.util import create_shell_command_asset


class SpatialDataset(NamedTuple):
    name: str
    url: str
    relative_path: str | None = None


SPATIAL_DATASETS = [
    SpatialDataset('nyc_congressional_district_geometries', OPENDATA_SPATIAL_URL.format(id='62dw-nwnq')),
    SpatialDataset('nyc_senate_district_geometries', OPENDATA_SPATIAL_URL.format(id='h4i2-acfi')),
    SpatialDataset('nyc_assembly_district_geometries', OPENDATA_SPATIAL_URL.format(id='qh62-9utz')),
    SpatialDataset('nyc_city_council_district_geometries', OPENDATA_SPATIAL_URL.format(id='jgqm-ccbd')),
    SpatialDataset('nyc_community_district_geometries', OPENDATA_SPATIAL_URL.format(id= 'mzpm-a6vd')),
    SpatialDataset('zip_code_geometries', f'{OPENDATA_URL}/download/i8iw-xf4u/application%2Fzip'),
    SpatialDataset('uhf_geometries', 'https://www.nyc.gov/assets/doh/downloads/zip/uhf42_dohmh_2009.zip', relative_path='UHF_42_DOHMH_2009'),
    SpatialDataset('nypd_precinct_geometries', OPENDATA_SPATIAL_URL.format(id='78dh-3ptz')),
    SpatialDataset('census_tract_geometries', NYC_GOV_API_URL.format(id='nyct2020_22c'), relative_path='nyct2020_22c'),
    SpatialDataset('census_block_geometries', NYC_GOV_API_URL.format(id='nycb2020_22c'), relative_path='nycb2020_22c')
]


def build_spatial_assets(d: SpatialDataset):
    zip_path = DATA_PATH / (d.name + '.zip')
    data_path = DATA_PATH / d.name
    shapefile_dir = DATA_PATH / d.name / d.relative_path if d.relative_path else data_path
    group = 'geo'

    z_name = d.name + '_zip'
    @asset(name=z_name, group_name=group, metadata={'source': d.url})
    def download_file() -> None:
        r = requests.get(url=d.url)
        with open(zip_path, 'wb') as f:
            f.write(r.content)

    sh_name = d.name + '_shapefile'
    @asset(name=sh_name, group_name=group, non_argument_deps={z_name})
    def extract_file() -> None:
        with ZipFile(zip_path) as z:
            z.extractall(data_path)

    load_spatial_data = create_shell_command_asset(
         f"{SCRIPTS_DIR}/load_geo_data.sh {shapefile_dir} {d.name}",
         name=d.name, group_name=group, non_argument_deps={sh_name}
    )
    return [download_file, extract_file, load_spatial_data]

opendata_assets = [
    a for d in SPATIAL_DATASETS for a in build_spatial_assets(d)
]
