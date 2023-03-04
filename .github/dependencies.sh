#!/bin/bash -e

sudo add-apt-repository ppa:ubuntugis/ppa
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  gdal-bin \
  libgdal-dev \
  python3-gdal \
  python3.8 \
  python3.8-dev \
  sqlite3 \
  unzip \
  zip
pip install -r requirements.txt
python setup.py
