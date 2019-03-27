echo "Clearing out existing data..."
rm elevation.mbtiles
psql underfoot -c "drop table contours12"
find elevation-tiles/ -type f -name '*.merge*' -delete

echo "Make the contours and load them into PostGIS (you can probably ignore the errors)..."
node elevation.js 12
