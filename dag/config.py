import os
from pathlib import Path
from dagster import file_relative_path

DBT_PROJECT_PATH = file_relative_path(__file__, "../dbt")
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR')
SCRIPTS_DIR = file_relative_path(__file__, "../scripts")
DATA_DIR = file_relative_path(__file__, "../.data")
DATA_PATH = Path(DATA_DIR)