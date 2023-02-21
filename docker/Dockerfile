FROM python:3.10-bullseye

COPY requirements/ /opt/dagster/requirements/

RUN pip install --upgrade pip \
    && python3 -m pip install --upgrade setuptools wheel \
    && GRPC_PYTHON_BUILD_SYSTEM_ZLIB=true pip install -r /opt/dagster/requirements/requirements_dagster.txt

WORKDIR /opt/dagster/app

COPY . .
