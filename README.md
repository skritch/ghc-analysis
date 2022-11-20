

# GHC Analysis

This repo contains data processing and analysis for the [Greater Harlem Coalition](https://greaterharlem.nyc/).

* `/dbt` contains a [DBT](https://www.getdbt.com/) project which ingests data (from various sources copied as seed files) and transformation code, written in SQL. It is written to run on a Postgres database. (DBT is overkill for this but it's a pretty good way to manage SQL, and SQL is way better
at documenting what's actually going on than spreadsheets.)

* `/analysis` contains data analysis, as Jupyter notebooks.

## Installation

To setup, you'll need:
* PostgreSQL
* Python (I use 3.10.4).

The basic installation steps are:
1. Install `requirements.txt` in a Python environment via Pip or Conda. (I use Conda because Pip can't figure out `pymc` on my machine, but I'm barely using that in one notebook, so you can skip it.)
2. Setup `nbdev` for git-friendly Jupyter notebooks:
```
nbdev_install_hooks
```

3. Create a Postgres database for the project, and create a file `dbt/profiles.yml` to point to it (see [docs](https://docs.getdbt.com/docs/get-started/connection-profiles))


4. Run DBT to populate your database:
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

5. Set a DB_URL corresponding to your database and run the notebooks, for example:

```
export DB_URL='postgresql://harlem:harlem@localhost:5432/harlem'

jupyter notebook
```

