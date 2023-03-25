
from typing import NamedTuple

import requests
from dagster import asset

from dag import postgres
from dag.assets.constants import OPENDATA_CSV_URL
from dag.config import DATA_PATH


class CSVDataset(NamedTuple):
    name: str
    url: str
    schema: list[tuple[str, type]]
    group_name: str | None = None

CSV_DATASETS = [
    CSVDataset(
        'nyc_city_council_members', OPENDATA_CSV_URL.format(id='uvw5-9znb'),
        [('name', str), ('district', int), ('borough', str), ('political_party', str)]
    )
]

# CSV_DATASETS = {
    # 'nyc_congressional_district_demographics': OPENDATA_CSV_URL.format(id='77d2-9ebr'),
    # 'nyc_senate_district_demographics': OPENDATA_CSV_URL.format(id='uv67-wxba'),
    # 'nyc_community_district_demographics': OPENDATA_CSV_URL.format(id='w3c6-35wg'),
# }

def build_csv_seed_assets(d: CSVDataset):
    """
    Downloads a CSV, which we then need to load.
    """
    data_path = DATA_PATH / (d.name + '.csv')
    url = d.url

    csv_asset_name = d.name + '_csv'
    @asset(name=csv_asset_name, metadata={'source': url})
    def download_file() -> None:
        r = requests.get(url=url)
        with open(data_path, 'wb') as f:
            f.write(r.content)


    @asset(name=d.name, metadata={'source': url}, non_argument_deps={csv_asset_name})
    def load_csv() -> None:
        postgres.load_csv(postgres.Schema.from_python(d.schema, d.name), data_path)
    return [download_file, load_csv]

csv_assets = [
    a for n in CSV_DATASETS for a in build_csv_seed_assets(n) 
]
