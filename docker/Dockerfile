FROM python:3.10-bullseye

RUN apt-get update && apt-get install -y gdal-bin python3-gdal

COPY requirements/ /opt/dagster/requirements/

RUN pip install --upgrade pip \
    && python3 -m pip install --upgrade setuptools wheel \
    && GRPC_PYTHON_BUILD_SYSTEM_ZLIB=true \
       CPLUS_INCLUDE_PATH=/usr/include/gdal \
       C_INCLUDE_PATH=/usr/include/gdal \
    pip install -r /opt/dagster/requirements/requirements_dagster.txt

WORKDIR /opt/dagster/app

COPY . .

RUN mkdir ~/.dbt/ && cp docker/dbt_profiles.yml  ~/.dbt/profiles.yml
