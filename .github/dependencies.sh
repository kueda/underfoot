#!/bin/bash -e

sudo add-apt-repository ppa:ubuntugis/ppa
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
  python3-gdal \
  python3-pip \
  sqlite3 \
  unzip \
  virtualenv \
  zip
