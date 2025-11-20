#!/bin/bash -e

echo "UPDATING apt-get..."
sudo apt-get update
echo "INSTALLING APT PACKAGES..."
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

echo "COMPILING AND INSTALLING IMPOSM DEPENDENCIES..."
sudo apt-get install -y golang-go libleveldb-dev

echo "REINSTALLING NUMPY 2"
pip uninstall numpy
pip install "numpy<2.0"

echo "INSTALLING GDAL PYTHON BINDINGS..."
pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"

echo "FINISHED INSTALLING SYSTEM PACKAGES"
