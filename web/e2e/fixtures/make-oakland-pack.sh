#!/usr/bin/env bash
# Rebuilds us-ca-oakland.pmtiles.zip, a cut-down copy of the real us-ca-oakland pack for the
# e2e tests. It keeps only the tiles the app needs to show the spot where it first opens the
# pack, so it's small enough to commit. Needs the pmtiles CLI:
# https://github.com/protomaps/go-pmtiles
set -euo pipefail

cd "$(dirname "$0")"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -sSf -o "$work/full.zip" https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip
unzip -q "$work/full.zip" -d "$work/full"
full="$work/full/us-ca-oakland"
cut="$work/us-ca-oakland"
mkdir "$cut"

# pmtiles extract centers each archive on this box, and the app centers the map on the ways
# archive's center
bbox=-122.1905,37.8145,-122.1895,37.8155
for layer in context contours rocks water; do
  pmtiles extract -q --bbox=$bbox --minzoom=11 --maxzoom=11 "$full/$layer.pmtiles" \
    "$cut/$layer.pmtiles"
done
# The app opens a pack at its ways archive's max zoom minus 2, so this keeps it opening at 11
pmtiles extract -q --bbox=$bbox --minzoom=13 --maxzoom=13 "$full/ways.pmtiles" \
  "$cut/ways.pmtiles"
# Leaves out water-waterways_network.csv, which is large and which the app doesn't read
cp "$full/rocks-citations.csv" "$full/rocks-rock_units_attrs.csv" "$full/water-citations.csv" \
  "$cut/"

(cd "$work" && zip -q -r - us-ca-oakland) > us-ca-oakland.pmtiles.zip
