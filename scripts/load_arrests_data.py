"""
Load arrest data from NYC OpenData, e.g. 
https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u/data
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
        row['ARREST_KEY'],
        datetime.datetime.strptime(row['ARREST_DATE'], '%m/%d/%Y').date(),
        row['OFNS_DESC'],
        row['PD_DESC'],
        row['LAW_CODE'],
        row['LAW_CAT_CD'],
        row['ARREST_BORO'],
        row['ARREST_PRECINCT'],
        row['JURISDICTION_CODE'],
        # row['AGE_GROUP'],
        # row['PERP_RACE'],
        row['Latitude'],
        row['Longitude']
    )

def load(f, table):
    reader = csv.DictReader(f)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        arrest_key TEXT PRIMARY KEY,
        arrest_date DATE NOT NULL,
        offense_description TEXT,
        pd_description TEXT,
        law_code TEXT,
        offense_level VARCHAR(1),
        boro VARCHAR(1),
        precinct SMALLINT,
        jurisdiction_code SMALLINT,
        latitude FLOAT,
        longitude FLOAT
    )
    """

    with psycopg2.connect(db_url) as con, \
        con.cursor() as cur:
        cur.execute(create_table_sql)
        cur.execute(f'DELETE FROM {table}')

        psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {table} VALUES %s ",
                (transform_row(row) for row in reader),
                page_size=10000,
            )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str, default='nypd_arrests')
    parser.add_argument('file', type=argparse.FileType('r'))
    args = parser.parse_args()
    load(args.file, args.table)