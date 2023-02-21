

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE USER dagster PASSWORD 'dagster';
CREATE DATABASE dagster OWNER dagster;
EOSQL

