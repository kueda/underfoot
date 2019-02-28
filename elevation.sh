echo "Clearing out existing data..."
rm elevation.mbtiles
psql underfoot -c "drop table contours8"
psql underfoot -c "drop table contours10"
find elevation-tiles/ -type f -name '*.merge*' -delete

echo "Make the contours and load them into PostGIS (you can probably ignore the errors)..."
node elevation.js 8
node elevation.js 10
