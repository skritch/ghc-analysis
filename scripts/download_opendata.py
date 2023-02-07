from enum import Enum
from pathlib import Path
from typing import NamedTuple
from zipfile import ZipFile

import requests


OPENDATA_URL = 'https://data.cityofnewyork.us'
OPENDATA_UI_URL = f'{OPENDATA_URL}/d/{{id}}'
OPENDATA_SPATIAL_URL = f'{OPENDATA_URL}/api/geospatial/{{id}}?method=export&format=Shapefile'
OPENDATA_CSV_URL = f'{OPENDATA_URL}/api/views/{{id}}/rows.csv?accessType=DOWNLOAD'
DATA_PATH = Path('./data')

SPATIAL_DATASETS = {
    'nyc_congressional_district_geometries': OPENDATA_SPATIAL_URL.format(id='62dw-nwnq'),
    'nyc_senate_district_geometries': OPENDATA_SPATIAL_URL.format(id='h4i2-acfi'),
    'nyc_assembly_district_geometries': OPENDATA_SPATIAL_URL.format(id='qh62-9utz'),
    'nyc_city_council_district_geometries':  OPENDATA_SPATIAL_URL.format(id='jgqm-ccbd'),
    'nyc_community_district_geometries': OPENDATA_SPATIAL_URL.format(id= 'mzpm-a6vd'),
    'zip_code_geometries': f'{OPENDATA_URL}/download/i8iw-xf4u/application%2Fzip',
    'uhf_geometries': 'https://www.nyc.gov/assets/doh/downloads/zip/uhf42_dohmh_2009.zip',
    'nypd_precinct_geometries': OPENDATA_SPATIAL_URL.format(id='78dh-3ptz')
}

CSV_DATASETS = {
    'nyc_congressional_district_demographics': OPENDATA_CSV_URL.format(id='77d2-9ebr'),
    'nyc_senate_district_demographics': OPENDATA_CSV_URL.format(id='uv67-wxba'),
    'nyc_community_district_demographics': OPENDATA_CSV_URL.format(id='w3c6-35wg'),
    'nyc_city_council_members': OPENDATA_CSV_URL.format(id='uvw5-9znb')
}


def dl_extract_shapefile(name: str, url: str):
    zip_path = DATA_PATH / ((name) + '.zip')
    print(f'Downloading {url}...')
    r = requests.get(url=url)
    with open(zip_path, 'wb') as f:
        f.write(r.content)
    
    print(f'Extracting {zip_path}...')
    new_path = DATA_PATH / name
    with ZipFile(zip_path) as z:
        z.extractall(new_path)

    print(new_path)

def dl_csv(name: str, url: str):
    csv_path = DATA_PATH / ((name) + '.csv')
    print(f'Downloading {url}...')
    r = requests.get(url=url)
    with open(csv_path, 'wb') as f:
        f.write(r.content)
    print(csv_path)

def download_all():
    for d, url in SPATIAL_DATASETS.items():
        dl_extract_shapefile(d, url)
    for d, url in CSV_DATASETS.items():
        dl_csv(d, url)
    

def cli(d: str | None = None, all=False):
    if all:
        download_all()
    elif d in SPATIAL_DATASETS:
        dl_extract_shapefile(d, SPATIAL_DATASETS[d])
    elif d in CSV_DATASETS:
        dl_csv(d, CSV_DATASETS[d])
    else:
        raise Exception(f"Requires a dataset name or all option")

if __name__ == '__main__':
  import fire
  fire.Fire(cli)