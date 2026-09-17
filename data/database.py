"""Functions for creating the database"""

import psycopg2
from sources import util

DBNAME = "underfoot"
DB_USER = "underfoot"
DB_PASSWORD = "underfoot"
SRID = "4326"


def make_database():
    """Create the database"""
    try:
        con = psycopg2.connect(f"dbname={DBNAME}")
    except psycopg2.OperationalError:
        util.call_cmd(["createdb", DBNAME])
        util.call_cmd(["psql", "-d", DBNAME, "-c", "CREATE EXTENSION postgis;"])
        con = psycopg2.connect(f"dbname={DBNAME}")
    con.close()
