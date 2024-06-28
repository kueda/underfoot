#!/bin/bash -e

sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo add-apt-repository ppa:deadsnakes/ppa
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

pip uninstall numpy
pip install "numpy<2.0"
pip install GDAL==$(gdal-config --version) --global-option=build_ext --global-option="-I/usr/include/gdal"
