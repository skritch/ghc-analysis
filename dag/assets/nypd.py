import datetime
from typing import Union

import requests
from dagster import asset

from dag.util import create_shell_command_asset

from dag.config import DATA_PATH, SCRIPTS_DIR
from .constants import OPENDATA_QUERY_URL, OPENDATA_UI_URL

years = list(range(2010, 2022))
current_year = 2022


class NypdArrests:
    name = 'nypd_arrests'
    group = 'nypd'
    prefix = 'arrests'
    historic_dataset_id = '8h9b-rp9u'
    historic_url_template = OPENDATA_QUERY_URL.format(id=historic_dataset_id) + \
        "?$where=arrest_date >= '{start:%Y-%m-%dT%H:%M}' and arrest_date < '{end:%Y-%m-%dT%H:%M}' and law_cat_cd != 'V'"
    
    current_url = OPENDATA_QUERY_URL.format(id='uip8-fykc') + \
        "?$where=law_cat_cd != 'V'"
    
    script_name = 'load_arrests_data.py'
    table = 'nypd_arrests'

    
class NypdComplaints:
    name = 'nypd_complaints'
    group = 'nypd'
    prefix = 'complaints'
    historic_dataset_id = 'qgea-i56i'
    historic_url_template = OPENDATA_QUERY_URL.format(id=historic_dataset_id) + \
        "?$where=cmplnt_fr_dt >= '{start:%Y-%m-%dT%H:%M}' and cmplnt_fr_dt < '{end:%Y-%m-%dT%H:%M}' and law_cat_cd != 'VIOLATION'"
    
    current_url = OPENDATA_QUERY_URL.format(id='5uac-w243') + \
        "?$where=law_cat_cd != 'V'"
    
    script_name = 'load_complaints_data.py'
    table = 'nypd_complaints'


def build_arrests_assets(year: int, config: NypdArrests | NypdComplaints, is_current = False):
    # Dagster partitions look complicated
    
    name = f'{config.name}_{year}'
    data_path = DATA_PATH / config.prefix / (str(year) + '.csv')

    if is_current:
        url = config.current_url
    else:
        start = datetime.date(year=year, month=1, day=1)
        end = datetime.date(year=year + 1, month=1, day=1)
        url = config.historic_url_template.format(start=start, end=end)

    csv_asset_name = name + '_csv'

    @asset(name=csv_asset_name, group_name=config.group, metadata={'source': url})
    def download_file() -> None:
        r = requests.get(url=url)
        if not data_path.parent.exists():
            data_path.parent.mkdir(parents=True, exist_ok=True)
        with open(data_path, 'wb') as f:
            f.write(r.content)

    load_data = create_shell_command_asset(
         f"python {SCRIPTS_DIR}/{config.script_name} --table {config.table} {data_path}",
         name=name + '_load', group_name=config.group, non_argument_deps={csv_asset_name}
    )
    return [download_file, load_data]


arrest_assets = build_arrests_assets(2022, config=NypdArrests(), is_current=True)
complaints_assets = build_arrests_assets(2022, config=NypdComplaints(), is_current=True)
for yr in range(2010, 2022):
    arrest_assets.extend(build_arrests_assets(yr, config=NypdArrests()))
    complaints_assets.extend(build_arrests_assets(yr, config=NypdComplaints()))

    
@asset(
    name=NypdArrests.name, 
    group_name=NypdArrests.group, 
    metadata={'source': OPENDATA_UI_URL.format(id=NypdArrests.historic_dataset_id)},
    non_argument_deps={
        f'{NypdArrests.name}_{yr}_load' for yr in years + [current_year]
    }
)
def arrests_wrapper():
    """Wrapper for yearly loads."""
    return

@asset(
    name=NypdComplaints.name, 
    group_name=NypdComplaints.group, 
    metadata={'source': OPENDATA_UI_URL.format(id=NypdComplaints.historic_dataset_id)},
    non_argument_deps={
        f'{NypdComplaints.name}_{yr}_load' for yr in years + [current_year]
    }
)
def complaints_wrapper():
    """Wrapper for yearly loads."""
    return
