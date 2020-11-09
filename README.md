# underfoot
Underfoot is a mobile app for revealing the hydrological and geological world beneath your feet. Well, sort of. It's mostly just something I tinker with in my spare time. It'll probably never be done. If you're interested in a more fully-functional app for geological exploration, check out [rockd](https://rockd.org).

Still reading? This repo is mostly for data prep. I used to have some Ionic-based mobile app stuff here but I've given up on Ionic and am currently just tinkering with a native Android app.

## Vagrant Setup
You'll need to install [Vagrant](https://www.vagrantup.com/) and [VirtualBox](https://www.virtualbox.org/).
```bash
git clone https://github.com/kueda/underfoot.git
cd underfoot
vagrant up # This will take a while
vagrant ssh

# Subsequent commands in the Vagrant VM

# This is kind of optional. You could just run the code from /vagrant/, i.e. the
# files on the host, but it might be a bit safer to clone them from the repo and
# work on a separate clone. Then you have the awkwardness of copying files back
# and forth if you're developing, or syncing folders
# (https://www.vagrantup.com/docs/synced-folders/basic_usage.html). Up to you.
git clone https://github.com/kueda/underfoot.git
cd underfoot

# Set up a python virtual environment
virtualenv venv -p python3.8
source venv/bin/activate

# Install deps and some stuff for working with ESRI Arc/Info coverages
# ./setup
python setup.py

# Make a pack
python packs.py us-ca-oakland
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
  1. `span`: geologic time span of unit formation, e.g. Cretaceous, extracted from source but generally verbatim
  1. `controlled_span`: geologic time span of unit based on a controlled vocabulary dereived from the [Wikipedia "Period start" template](https://en.wikipedia.org/w/index.php?title=Template:Period_start&action=edit)
  1. `min_age`: Minimum age of the unit
  1. `max_age`: Maximum age of the unit
  1. `est_age`: Estimated age of the unit
1. A JSON file named `citation.json` containing a single-item array of [CSL Data](https://github.com/citation-style-language/schema/blob/master/csl-data.json) items of the kind exported from Zotero.

Sources can be single scripts or modules. Preferrably they will contain as little data as possible and instead download and transorm open data from the Internet, e.g. USGS publications, though some data will probably be necessary since few sources are fully machine-readable. There are numerous helper methods for writing source scripts in the `sources/util` module.

If you'd like to contribute a source, please file an issue first proposing a new source, and if we agree it should be added, work on a feature branch in your own fork and issue a pull request when it's ready. If that sounds like too much coding, file an issue anyway and I might put together a script if you perform some of the digitization / transcription.
