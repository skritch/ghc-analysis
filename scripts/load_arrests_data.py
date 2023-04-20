"""
Load arrest data from NYC OpenData, e.g. 
https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u/data

It would probably be better to do load the 
raw CSVs, along with a partition key to delete by,
and do these transformations inside the database.
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
        row['arrest_key'],
        datetime.datetime.fromisoformat(row['arrest_date']).date(),
        row['ofns_desc'],
        row['pd_desc'],
        row['law_code'],
        row['law_cat_cd'],
        row['arrest_boro'],
        int(float(row['arrest_precinct'])),
        int(float(row['jurisdiction_code'])),
        # row['AGE_GROUP'],
        # row['PERP_RACE'],
        row['latitude'],
        row['longitude']
    )

def load(f, table, replace):
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
    parser.add_argument('--table', type=str, default='nypd_arrests')
    parser.add_argument('file', type=argparse.FileType('r'))
    parser.add_argument('--replace', action='store_true')
    args = parser.parse_args()
    load(args.file, args.table, args.replace)