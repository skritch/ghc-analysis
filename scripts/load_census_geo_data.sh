#!/bin/bash

# usage: 
# make sure PostGIS is installed on the DB (requires super or something)
# so that ogr2ogr pre-creates the geometry column
# sh load_census_geo_data.sh <input filename> <table name>
ogr2ogr -update \
    -overwrite \
    -f PostgreSQL PG:"$DB_URL" $1 \
    -nlt MULTIPOLYGON25D \
    -nln $2 \
    -lco precision=NO \
    -progress