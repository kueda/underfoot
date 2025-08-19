#!/bin/bash -e

sudo apt-get update
sudo apt-get install -y \
  build-essential \
  gdal-bin \
  git \
  libgdal-dev \
  mdbtools-dev \
  odbc-mdbtools \
  postgis \
  python-is-python3 \
  python3 \
  python3-dev \
  python3-pip \
  sqlite3 \
  unzip \
  virtualenv \
  zip

# Compile and install imposm dependencies
sudo apt-get install -y golang-go libleveldb-dev

# Reinstall numpy
pip uninstall numpy
pip install "numpy<2.0"

# Install GDAL python bindings
pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
