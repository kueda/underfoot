#!/bin/bash

# Make a PostGIS database for the OSM data using the omsosis schema. Note that
# those osmosis files should  have been installed with osmosis when the VM was
# provisioned.
echo ""
echo "Creating the database and schema"
dropdb underfoot_ways
createdb underfoot_ways
psql -d underfoot_ways -c 'CREATE EXTENSION postgis; CREATE EXTENSION hstore;'
psql -d underfoot_ways < /usr/share/doc/osmosis/examples/pgsnapshot_schema_0.6.sql
psql -d underfoot_ways < /usr/share/doc/osmosis/examples/pgsnapshot_schema_0.6_bbox.sql
psql -d underfoot_ways < /usr/share/doc/osmosis/examples/pgsnapshot_schema_0.6_linestring.sql

# for state in arizona california # if you want to make an osm export for multiple states
for state in california
do
  echo ""
  echo "Downloading an export of recent $state OSM data (thanks, Geofabrik!)"
  if ! [ -e $state-latest.osm.pbf ]
  then
    curl -o $state-latest.osm.pbf http://download.geofabrik.de/north-america/us/$state-latest.osm.pbf
  fi
  # Load ways from the PBF into the database
  echo ""
  echo "Loading data from the PBF into the database"
  osmosis \
    --read-pbf $state-latest.osm.pbf \
    --tf accept-ways highway=* \
    --tf reject-ways service=* \
    --tf reject-ways footway=sidewalk \
    --tf reject-ways highway=proposed \
    --tf reject-ways highway=footway \
    --tf reject-ways highway=pedestrian \
    --tf reject-ways highway=steps \
    --write-pgsql database="underfoot_ways" user="underfoot" password="underfoot"
done

# Create a table for ways with just names and highway tags
echo ""
echo "Creating a table for ways with just names and highway tags"
psql underfoot_ways -c "CREATE TABLE underfoot_ways AS SELECT id, version, tags -> 'name' AS name, tags -> 'highway' AS highway, linestring FROM ways"

# Export ways into the MBTiles using different zoom levels for different types
echo ""
echo "Exporting into MBTiles"
rm ./underfoot_ways.mbtiles
./node_modules/tl/bin/tl.js copy -i underfoot_ways.json -z 3 -Z 13 \
  "postgis://underfoot:underfoot@localhost:5432/underfoot_ways?table=underfoot_ways&query=(SELECT%20*%20from%20underfoot_ways%20WHERE%20highway%20in%20('motorway'))%20AS%20foo" \
  mbtiles://./underfoot_ways.mbtiles
./node_modules/tl/bin/tl.js copy -i underfoot_ways.json -z 7 -Z 13 \
  "postgis://underfoot:underfoot@localhost:5432/underfoot_ways?table=underfoot_ways&query=(SELECT%20*%20from%20underfoot_ways%20WHERE%20highway%20in%20('motorway','primary','trunk'))%20AS%20foo" \
  mbtiles://./underfoot_ways.mbtiles
./node_modules/tl/bin/tl.js copy -i underfoot_ways.json -z 11 -Z 13 \
  "postgis://underfoot:underfoot@localhost:5432/underfoot_ways?table=underfoot_ways&query=(SELECT%20*%20from%20underfoot_ways%20WHERE%20highway%20in%20('motorway','primary','trunk','secondary','tertiary','motorway_link'))%20AS%20foo" \
  mbtiles://./underfoot_ways.mbtiles
./node_modules/tl/bin/tl.js copy -i underfoot_ways.json -z 13 -Z 13 \
  "postgis://underfoot:underfoot@localhost:5432/underfoot_ways?table=underfoot_ways" \
  mbtiles://./underfoot_ways.mbtiles
