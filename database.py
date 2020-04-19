import psycopg2
from sources import util

DBNAME = "underfoot"
SRID = "4326"

def make_database():
  try:
    con = psycopg2.connect("dbname={}".format(DBNAME))
  except psycopg2.OperationalError:
    util.call_cmd(["createdb", DBNAME])
    util.call_cmd(["psql", "-d", DBNAME, "-c", "CREATE EXTENSION postgis;"])
    con = psycopg2.connect("dbname={}".format(DBNAME))
  con.close()
