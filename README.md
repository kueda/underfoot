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
node tileserver.js
node cachetiles.js
find tiles/ -type d -empty -delete
```
