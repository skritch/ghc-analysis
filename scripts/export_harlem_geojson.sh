
ogr2ogr -f GeoJSON out.json \
  PG:"$DB_URL" \
  -sql "select
    bctcb2020,
    boundary::geometry as \"geometry\"
from census_blocks
where borough_district_code in (109, 110, 111)
  or (borough_district_code = 107 and bctcb2020::bigint >= 10180000000)"