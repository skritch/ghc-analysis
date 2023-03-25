

# GHC Analysis

This repo contains data processing and analysis for the [Greater Harlem Coalition](https://greaterharlem.nyc/).

* `/dbt` contains a [DBT](https://www.getdbt.com/) project which ingests data (from various sources copied as seed files) and transformation code, written in SQL. It is written to run on a Postgres database. (DBT is overkill for this but it's a pretty good way to manage SQL, and SQL is way better
at documenting what's actually going on than spreadsheets.)

* `/analysis` contains data analysis, as Jupyter notebooks.

## Docker

Install `docker` for your platform, then run:
```
docker-compose -f docker/docker-compose.yml up --build
```

## Local Installation

To setup, you'll need to configure:
* PostgreSQL
* Python (I use 3.10.4)
* DBT
* dagster

These aren't full installation instructions, you'll need to figure some things out.

The basic installation steps are:

1. Install Postgres with PostGIS and user for the project and set up permissions. Set the `DB_URL` envar. 

2. Install `requirements/requirements_dagster.txt` in a Python environment via Pip or Conda. 
    * Optionally, setup `nbdev` for git-friendly Jupyter notebooks:
    ```
    nbdev_install_hooks
    ```
    * Currently this project installs the dependencies for everything in one environment, which probably will cause some problems.

3. Create a file `dbt/profiles.yml` to point it to your database. (see [docs](https://docs.getdbt.com/docs/get-started/connection-profiles))

    * You can run DBT directly to populate your database:
    ```
    cd dbt
    export DBT_PROFILES_DIR=.
    dbt seed
    dbt run
    ```

    You can use DBT's web UI to browse the available datasets via:
    ```
    dbt docs generate
    dbt docs serve
    ```

4. Set up Dagster. You probably want to provision a different Postgres user and DB for it, or it will create a bunch of tables in the `public` schema. Set `DAGSTER_DB_URL` and run `dagster dev` to view your pipelines locally.
    * Dagster will read your DBT environment—you can run DBT models from within the Dagster UI.
    * Many of the upstream dependencies are only implemented in Dagster, but can also by run manually (in `scripts/`)
    * Not all steps are implemented; in some cases you'll need to read the error msg and perform some manual steps.
    * Dagster does not know about steps you run outside of its framework.

