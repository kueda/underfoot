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
