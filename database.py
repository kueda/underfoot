import psycopg2

DBNAME = "underfoot"
SRID = "4326"

def make_database():
  try:
    con = psycopg2.connect("dbname={}".format(DBNAME))
  except psycopg2.OperationalError:
    util.call_cmd(["createdb", dbname])
    util.call_cmd(["psql", "-d", dbname, "-c", "CREATE EXTENSION postgis;"])
    con = psycopg2.connect("dbname={}".format(DBNAME))
  con.close()
