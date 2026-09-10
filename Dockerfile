# syntax=docker/dockerfile:1
#
# The underfoot build environment: GDAL, a PostGIS client, imposm, e00conv, and
# the Python venv, on the same Ubuntu 24.04 / GDAL 3.8 base the CI runner uses.
# The app code is NOT copied in; it is bind-mounted at /app by compose.yaml and CI.

# ---- builder: compile the native tools ----
FROM ubuntu:24.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      git \
      golang-go \
      libgeos-dev \
      libleveldb-dev \
      libsnappy-dev \
      make \
    && rm -rf /var/lib/apt/lists/*

# imposm3 publishes only linux-x86-64 release binaries, so build from source
# (Go cross-compiles cleanly to arm64; leveldb/snappy are arch-independent C++).
ARG IMPOSM3_REF=v0.14.2
RUN git clone --branch "$IMPOSM3_REF" --depth 1 \
      https://github.com/omniscale/imposm3.git /src/imposm3 \
    && make -C /src/imposm3 build \
    && install -Dm755 /src/imposm3/imposm /out/bin/imposm

# e00conv decompresses ArcInfo .e00 files for some geologic sources. Portable
# ANSI C, no dependencies. Tarball is vendored (see vendor/README.md).
COPY vendor/e00compr-1.0.1.tar.gz /src/
RUN tar xzf /src/e00compr-1.0.1.tar.gz -C /src \
    && make -C /src/e00compr-1.0.1 e00conv \
    && install -Dm755 /src/e00compr-1.0.1/e00conv /out/bin/e00conv


# ---- pydeps: build the Python virtualenv ----
FROM ubuntu:24.04 AS pydeps

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      libgdal-dev \
      libgeos-dev \
      libpq-dev \
      python3 \
      python3-dev \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
RUN pip install --no-cache-dir --upgrade pip
# psycopg2 always compiles (source dist, not psycopg2-binary). Fiona has no arm64
# wheel, so it compiles against system GDAL 3.8 (libgdal-dev); the resulting .so
# links libgdal.so.34, which gdal-bin provides in the runtime stage. Shapely /
# rasterio resolve to arm64 wheels. libgeos-dev is a safety net for a Shapely
# source build.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt


# ---- runtime: the image the pipeline runs in ----
FROM ubuntu:24.04 AS runtime

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gdal-bin \
      git \
      gzip \
      libgeos-c1t64 \
      libleveldb1d \
      libpq5 \
      locales \
      mdbtools \
      odbc-mdbtools \
      poppler-utils \
      postgresql-client-16 \
      python3 \
      python3-gdal \
      sqlite3 \
      tar \
      unzip \
      zip \
    && rm -rf /var/lib/apt/lists/* \
    && locale-gen en_US.UTF-8

# gdal-bin: ogr2ogr, gdal_contour, ogrinfo (GDAL 3.8.4, MVT/PMTiles write driver,
#   SQLite/Spatialite dialect, GEOS, OpenFileGDB).
# python3-gdal: /usr/bin/gdal_merge.py (the hardcoded fallback in elevation.py).
# Nothing in the Python source imports osgeo, so there is no pip GDAL build.

# GDAL's ODBC driver auto-registers MDB Tools by searching only the amd64
# multiarch lib dir, so .mdb sources (sim3109, sim3206) fail on arm64. Register
# it explicitly under the name GDAL's .mdb connection strings ask for.
RUN mdbodbc="$(find /usr/lib -name libmdbodbc.so -print -quit)" \
    && { \
      echo ''; \
      echo '[Microsoft Access Driver (*.mdb, *.accdb)]'; \
      echo 'Description=MDB Tools ODBC'; \
      echo "Driver=$mdbodbc"; \
      echo "Setup=$mdbodbc"; \
      echo 'FileUsage=1'; \
      echo 'UsageCount=1'; \
    } >> /etc/odbcinst.ini

COPY --from=builder /out/bin/imposm  /usr/local/bin/imposm
COPY --from=builder /out/bin/e00conv /usr/local/bin/e00conv
COPY --from=pydeps  /opt/venv        /opt/venv

ENV PATH=/opt/venv/bin:$PATH \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    PGHOST=db \
    PGPORT=5432 \
    PGUSER=underfoot \
    PGPASSWORD=underfoot \
    UNDERFOOT_IMPOSM=/usr/local/bin/imposm \
    UNDERFOOT_E00CONV=/usr/local/bin/e00conv

WORKDIR /app
CMD ["bash"]
