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

class SpatialDataset(Enum):
    nyc_congressional_district_geometries = '62dw-nwnq'
    nyc_senate_district_geometries = 'h4i2-acfi'
    nyc_assembly_district_geometries = 'qh62-9utz'
    nyc_community_districts_geometries = 'mzpm-a6vd'
    nyc_city_council_district_geometries = 'jgqm-ccbd'


class CSVDataset(Enum):
    nyc_congressional_district_demographics = '77d2-9ebr'
    nyc_senate_district_demographics = 'uv67-wxba'
    nyc_community_district_demographics = 'w3c6-35wg'


def dl_extract_shapefile(d: SpatialDataset):
    zip_path = DATA_PATH / ((d.name) + '.zip')
    url = OPENDATA_SPATIAL_URL.format(id=d.value)
    print(f'Downloading {url}...')
    r = requests.get(url=url)
    with open(zip_path, 'wb') as f:
        f.write(r.content)
    
    new_path = DATA_PATH / d.name
    with ZipFile(zip_path) as z:
        z.extractall(new_path)

    return new_path

def dl_csv(d: CSVDataset):
    csv_path = DATA_PATH / ((d.name) + '.csv')
    url = OPENDATA_CSV_URL.format(id=d.value)
    print(f'Downloading {url}...')
    r = requests.get(url=url)
    with open(csv_path, 'wb') as f:
        f.write(r.content)
    return csv_path


def cli(d: str):
    if d in SpatialDataset.__members__:
        return dl_extract_shapefile(SpatialDataset[d])
    elif d in CSVDataset.__members__:
        return dl_csv(CSVDataset[d])
    else:
        raise Exception(f"Unrecognized dataset {d}")

if __name__ == '__main__':
  import fire
  fire.Fire(cli)