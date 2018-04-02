# underfoot
Underfoot is a mobile app for revealing the hydrological and geological world beneath your feet. Well, sort of. It's mostly just something I tinker with in my spare time. It'll probably never be done. If you're interested in a more fully-functional app for geological exploration, check out [rockd](https://rockd.org).

Still reading? This repo is mostly for data prep. I used to have some Ionic-based mobile app stuff here but I've given up on Ionic and am currently just tinkering with a native Android app.

## Vagrant setup
You'll need to install [Vagrant](https://www.vagrantup.com/) and [VirtualBox](https://www.virtualbox.org/).
```
git clone https://github.com/kueda/underfoot.git
cd underfoot
vagrant up # This will take a while
vagrant ssh

# Subsequent commands in the Vagrant VM

# Now you need to clone within the VM. You *could* use the mounted repo at
# /vagrant, which will give you easy filesystem access from the host, but npm
# does not seem to build things like mapnik there (lots of ENOENT errors due to
# symlink problems with shared folders, see
# https://github.com/npm/npm/issues/9479), and I haven't figured out a fix, so
# to get *every* part of the process to work you need this separate checkout. If
# you need access to the files you generate from the host you can just move them
# to /vagrant or set up another synced folder a la
# https://www.vagrantup.com/docs/synced-folders/basic_usage.html

git clone https://github.com/kueda/underfoot.git
cd underfoot

# Set up a python virtual environment
virtualenv venv -p python3 --no-site-packages
source venv/bin/activate

# Install python deps and some stuff for working with ESRI Arc/Info coverages
./setup

# Create the database and prepare all the data. Takes a good long while.
python prepare-database.py

# Install node deps for generating MBTiles
npm install
npm install -g tl tilelive-postgis tilelive-mbtiles

# Generate MBtiles in the shared Vagrant synced folder
tl copy -z 7 -Z 14 \
  'postgis://ubuntu:ubuntu@localhost:5432/underfoot?table=units' \
  mbtiles:///vagrant/underfoot.mbtiles
```

## OS X Setup

```
# Instal GDAL for GIS data processing and PostGIS / PostgreSQL for data storage
brew install gdal --with-postgres
brew install postgis nodejs pyenv-virtualenv

# Set up a python virtual environment
unset PYTHONPATH
virtualenv venv -p python3 --no-site-packages
source venv/bin/activate

# Install python deps and some stuff for working with ESRI Arc/Info coverages
./setup

# Create the database
python prepare-database.py

# Install node deps for generating MBTiles
npm install
npm install -g tl tilelive-postgis tilelive-mbtiles

# Generate MBtiles in the shared Vagrant synced folder
tl copy -z 7 -Z 14 \
  'postgis://ubuntu:ubuntu@localhost:5432/underfoot?table=units' \
  mbtiles:///vagrant/underfoot.mbtiles
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
