#!/usr/bin/env bash
# Rebuilds us-ca-oakland.pmtiles.zip, a cut-down copy of the real us-ca-oakland pack for the
# e2e tests. It keeps only the tiles the app needs to show the spot where it first opens the
# pack, so it's small enough to commit. Needs the pmtiles CLI:
# https://github.com/protomaps/go-pmtiles
#
# Usage: make-oakland-pack.sh [pack.zip]
#
# Cuts down the published pack, or a local one like data/build/us-ca-oakland.pmtiles.zip, e.g.
# to test pack changes that haven't been published yet.
set -euo pipefail

local_pack=${1:+$(realpath "$1")}
cd "$(dirname "$0")"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ -n "$local_pack" ]; then
  cp "$local_pack" "$work/full.zip"
else
  curl -sSf -o "$work/full.zip" https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip
fi
unzip -q "$work/full.zip" -d "$work/full"
# The published pack keeps its files in us-ca-oakland/, and local builds in
# us-ca-oakland.pmtiles/
full=$(dirname "$work"/full/*/ways.pmtiles)
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
cp "$full/rocks-citations.csv" "$full/rocks-rock_units_attrs.csv" "$full/water-citations.csv" \
  "$cut/"

(cd "$work" && zip -q -r - us-ca-oakland) > us-ca-oakland.pmtiles.zip
