# Thin wrapper over `docker compose` for the common workflows.
# See AGENTS.md / README.md for what these do.

COMPOSE := docker compose
RUN := $(COMPOSE) run --rm app

.PHONY: help build up down shell pack list source test psql psql-osm clean clean-caches

help:
	@echo "make build         build the app image"
	@echo "make up / down     start / stop the Postgres service"
	@echo "make shell         interactive shell in the app container"
	@echo "make pack PACK=us-ca-oakland [ARGS='--only rocks water']"
	@echo "make list          list available pack ids"
	@echo "make source SOURCE=mf2342c    run one source module"
	@echo "make test [ARGS='-k something']"
	@echo "make psql / psql-osm          psql into the underfoot / underfoot_osm db"
	@echo "make clean         docker compose down -v (drops the pgdata volume)"
	@echo "make clean-caches  rm the on-disk source/elevation/osm/build caches"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d db

down:
	$(COMPOSE) down

shell:
	$(RUN) bash

pack:
	$(RUN) python packs.py $(PACK) $(ARGS)

list:
	$(RUN) python packs.py list

source:
	$(RUN) python sources/$(SOURCE).py

test:
	$(RUN) pytest -v $(ARGS)

psql:
	$(COMPOSE) up -d db
	$(COMPOSE) exec db psql -U underfoot -d underfoot

psql-osm:
	$(COMPOSE) up -d db
	$(COMPOSE) exec db psql -U underfoot -d underfoot_osm

clean:
	$(COMPOSE) down -v

clean-caches:
	rm -rf sources/work-* elevation-tiles build/* *.osm.pbf
