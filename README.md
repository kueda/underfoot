# underfoot
App for revealing the hydrological and geological world beneath your feet.

## Setup

```
## Instal GDAL for GIS data processing and PostGIS / PostgreSQL for data storage
brew install gdal postgis nodejs pyenv-virtualenv

## Set up a python virtual environment
unset PYTHONPATH
virtualenv venv -p python3 --no-site-packages
source venv/bin/activate

## Install python deps and some stuff for working with ESRI Arc/Info coverages
./setup

## Create the database
python prepare-database.py

# Run the tileserver, generate tiles, remove empty dirs
npm install
node tileserver.js
node cachetiles.js

# Set up the mobile app
cd UnderfootApp
npm install -g cordova
npm install

# Test the mobile app in the browser
ionic serve

# Run the mobile app on a connected Android device (requires the Android SDK, see Ionic docs)
ionic run android

# Run the mobile app in the iOS simulator (requires the Xcode, see Ionic docs)
ionic emulate ios
```

# Adding Sources

Each source is an executable Python script that creates a directory at `sources/work-SCRIPT_NAME` containing

1. A GeoJSON filed named `units.geojson` that contains all the geological map units, each of which contains the following properties:
  1. `code`: Code used to label the unit
  1. `title`
  1. `description`
  1. `lithology`: rocks included in this unit
  1. `rock_type`: igneous, sedimentary, or metamorphic
  1. `formation`
  1. `grouping`: i.e. the group, e.g. Franciscan Complex 
  1. `span`: geologic time span of unit formation, e.g. Cretaceous
  1. `min_age`: Minimum age of the unit
  1. `max_age`: Maximum age of the unit
  1. `est_age`: Estimated age of the unit
1. A JSON file named `citation.json` containing a single-item array of [CSL Data](https://github.com/citation-style-language/schema/blob/master/csl-data.json) items of the kind exported from Zotero.

Sources can be single scripts or modules. Preferrably they will contain as little data as possible and instead download and transorm open data from the Internet, e.g. USGS publications, though some data will probably be necessary since few sources are fully machine-readable. There are numerous helper methods for writing source scripts in the `sources/util` module.

If you'd like to contribute a source, please file an issue first proposing a new source, and if we agree it should be added, work on a feature branch in your own fork and issue a pull request when it's ready. If that sounds like too much coding, file an issue anyway and I might put together a script if you perform some of the digitization / transcription.
