FROM postgis/postgis:15-3.3

COPY docker/init_db.sh /docker-entrypoint-initdb.d/
