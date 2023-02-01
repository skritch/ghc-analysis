from enum import Enum
from pathlib import Path
from typing import NamedTuple
from zipfile import ZipFile

import requests


OPENDATA_URL = 'https://data.cityofnewyork.us'
OPENDATA_UI_URL = f'{OPENDATA_URL}/d/{{id}}'
OPENDATA_SPATIAL_URL = f'{OPENDATA_URL}/api/geospatial/{{id}}?method=export&format=Shapefile'
OPENDATA_CSV_URL = f'{OPENDATA_URL}/api/views/{{id}}/rows.csv?accessType=DOWNLOAD'
OPENDATA_ZIP_CODE_URL = f'{OPENDATA_URL}/download/i8iw-xf4u/application%2Fzip'
DATA_PATH = Path('./data')

class SpatialDataset(Enum):
    nyc_congressional_district_geometries = '62dw-nwnq'
    nyc_senate_district_geometries = 'h4i2-acfi'
    nyc_assembly_district_geometries = 'qh62-9utz'
    nyc_city_council_district_geometries = 'jgqm-ccbd'
    nyc_community_district_geometries = 'mzpm-a6vd'


class CSVDataset(Enum):
    nyc_congressional_district_demographics = '77d2-9ebr'
    nyc_senate_district_demographics = 'uv67-wxba'
    nyc_community_district_demographics = 'w3c6-35wg'
    nyc_city_council_members = 'uvw5-9znb'

ZIP = 'zip_code_geometries'


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
    for d in iter(SpatialDataset):
        dl_extract_shapefile(d.name, OPENDATA_SPATIAL_URL.format(id=d.value))
    for d in iter(CSVDataset):
        dl_csv(d.name, OPENDATA_CSV_URL.format(id=d.value))
    dl_extract_shapefile(ZIP, OPENDATA_ZIP_CODE_URL)
    

def cli(d: str | None = None, all=False):
    if all:
        download_all()
    elif d in SpatialDataset.__members__:
        id = SpatialDataset[d].value
        dl_extract_shapefile(d, OPENDATA_SPATIAL_URL.format(id=id))
    elif d in CSVDataset.__members__:
        id = CSVDataset[d].value
        dl_csv(d, OPENDATA_CSV_URL.format(id=id))
    elif d == ZIP:
        dl_extract_shapefile(ZIP, OPENDATA_ZIP_CODE_URL)
    else:
        raise Exception(f"Requires a dataset name or all option")

if __name__ == '__main__':
  import fire
  fire.Fire(cli)