"""
Load arrest data from NYC OpenData, e.g. 
https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
"""

import os 
import csv
import psycopg2
import psycopg2.extras
import argparse
import datetime

db_url = os.getenv('DB_URL')

def transform_row(row):
    return (
        row['cmplnt_num'],
        datetime.datetime.fromisoformat(f"{row['cmplnt_fr_dt']}").date(),
        row['cmplnt_fr_tm'],
        datetime.datetime.fromisoformat(row['rpt_dt']).date(),
        int(float(row['addr_pct_cd'])) if row['addr_pct_cd'] else None,
        # row['ky_cd'],
        row['ofns_desc'],
        row['pd_desc'],
        row['crm_atpt_cptd_cd'] == 'COMPLETED',
        'F' if row['law_cat_cd'] == 'FELONY' else 
            'M' if row['law_cat_cd'] == 'MISDEMEANOR' else 
            'V',
        1 if row['boro_nm'] == 'MANHATTAN' else
            2 if row['boro_nm'] == 'BRONX' else
            3 if row['boro_nm'] == 'BROOKLEN' else
            4 if row['boro_nm'] == 'QUEENS' else
            5,

        int(float(row['jurisdiction_code'])) if row['jurisdiction_code'] else None,
        row['loc_of_occur_desc'],
        row['prem_typ_desc'],
        row['latitude'] or None,
        row['longitude'] or None
    )

def load(f, table, replace):
    reader = csv.DictReader(f)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        complaint_id TEXT PRIMARY KEY,
        complaint_date TIMESTAMP NOT NULL,
        complaint_time TEXT NOT NULL,
        report_date DATE,
        precinct SMALLINT,
        offense_description TEXT,
        pd_description TEXT,
        offense_completed BOOL,
        offense_level VARCHAR(1),
        boro_code VARCHAR(1),
        jurisdiction_code SMALLINT,
        location_description TEXT,
        premises_description TEXT,
        latitude FLOAT,
        longitude FLOAT
    )
    """

    with psycopg2.connect(db_url) as con, \
        con.cursor() as cur:
        cur.execute(create_table_sql)
        if replace:
            cur.execute(f'DELETE FROM {table}')

        psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {table} VALUES %s ON CONFLICT DO NOTHING",
                (transform_row(row) for row in reader),
                page_size=10000,
            )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str, default='nypd_complaints')
    parser.add_argument('file', type=argparse.FileType('r'))
    parser.add_argument('--replace', action='store_true')
    args = parser.parse_args()
    load(args.file, args.table, args.replace)