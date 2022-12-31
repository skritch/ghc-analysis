import os
import csv
from typing import NamedTuple, TypeAlias
import psycopg2
import psycopg2.extras
import argparse
import datetime
from here import *

db_url = os.getenv('DB_URL')


ZipCode: TypeAlias = str

class LatLon(NamedTuple):
    latitude: float
    longitude: float

# origin "{lat},{lng}" destination "{lat},{lng}"
# metadata – a notices (optional) object with a list of issues related to the response
# response – a routes object with a list of sections for each transit route alternative.



def extract_all_zip_codes():
    q = """
    SELECT 
        zip_code, centroid_latitude, centroid_longitude
    FROM zip_codes
    WHERE centroid IS NOT NULL
    """
    with psycopg2.connect(db_url) as con, \
        con.cursor() as cur:
            cur.execute(q)
            return {r[0]: LatLon(r[1], r[2]) for r in cur.fetchall()}


class ZipPair(NamedTuple):
    from_zip_code: ZipCode
    to_zip_code: ZipCode
    from_latlon: LatLon
    to_latlon: LatLon


def extract_nearest_zip_codes() -> list[ZipPair]:
    q = """
    with ds as (
        select
            z1.zip_code as from_zip_code,
            z2.zip_code as to_zip_code,
            z1.centroid_latitude,
            z1.centroid_longitude,
            z2.centroid_latitude,
            z2.centroid_longitude,
            ST_Distance(z1.centroid, z2.centroid) as distance_meters,
            ntile(6) over (partition by z1.zip_code order by ST_Distance(z1.centroid, z2.centroid)) as percentile_bucket 
        from zip_codes as z1
            cross join zip_codes as z2
        where z1.zip_code != z2.zip_code
    )
    select *
    from ds
    where percentile_bucket = 1
    """
    with psycopg2.connect(db_url) as con, \
        con.cursor() as cur:
            cur.execute(q)
            return [
                ZipPair(
                    r[0],
                    r[1],
                    LatLon(r[2], r[3]),
                    LatLon(r[4], r[5])
                ) for r in cur.fetchall()
            ]


def write_travel_times(travel_times: list[tuple[ZipCode, ZipCode, float]], table: str):
    create_q = f"""
    DROP TABLE IF EXISTS {table};
    CREATE TABLE {table} (
        from_zip_code TEXT NOT NULL,
        to_zip_code TEXT NOT NULL,
        travel_time FLOAT NOT NULL,
        PRIMARY KEY (from_zip_code, to_zip_code)
    );
    """
    with psycopg2.connect(db_url) as con, \
        con.cursor() as cur:
            cur.execute(create_q)

            psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {table} VALUES %s ",
                    travel_times,
                    page_size=10000,
                )

def run(table):
    """
    Note: this can spend money! And be slow.
    """
    # TODO: filter out ones we've already done?
    # grab all distances appearing in the one table.


    route_pairs = extract_nearest_zip_codes()
    print(len(route_pairs))

    route_pairs = route_pairs[:25]

    pairs = [
        ((r.from_latlon.latitude, r.from_latlon.longitude), (r.to_latlon.latitude, r.to_latlon.longitude))
        for r in route_pairs
    ]
    travel_times: list[float | None] = list(get_transit_travel_time(pairs))

    to_write = [
        (z1, z2, t)
        for ((z1, z2, _, _), t) in zip(route_pairs, travel_times)
        if t is not None
    ]
    write_travel_times(to_write, table)


def run_matrix(table):
    zips = list(extract_all_zip_codes().items())
    latlons = [(ll.latitude, ll.longitude) for z, ll in zips]
    travel_times = get_travel_time_matrix(latlons)
    travel_times_by_zip = [
        (zips[i][0], zips[f][0], t)
        for (i, f, t) in travel_times
    ]
    write_travel_times(travel_times_by_zip, table)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str, default='zip_code_distances')
    args = parser.parse_args()
    run_matrix(args.table)