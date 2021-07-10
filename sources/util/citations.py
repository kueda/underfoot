from . import make_work_dir, run_sql, log
from psycopg2.errors import UndefinedTable
import json
import os
import re

# TODO if this is going to get reused for other citations it should probably be
# in packs.py
citations_table_name = "citations"


def create_table():
    run_sql(f"""
      CREATE TABLE IF NOT EXISTS {citations_table_name} (
        source VARCHAR(255),
        citation TEXT)
    """)


def load_citation_for_source(source_identifier):
    # Delete existing row, create table if missing
    try:
        run_sql(f"""
            DELETE FROM {citations_table_name}
            WHERE source = '{source_identifier}'
        """)
    except UndefinedTable:
        create_table()
    path = os.path.join("sources", "{}.py".format(source_identifier))
    work_path = make_work_dir(path)
    citation_json_path = os.path.join(work_path, "citation.json")
    if not os.path.isfile(citation_json_path):
        return
    with open(citation_json_path) as f:
        citation_json = json.loads(f.read())
        c = citation_json[0]
        authorship = None
        if "author" in c:
            authorship = ""
            for idx, author in enumerate(c["author"]):
                if idx != 0:
                    if idx == len(c["author"]) - 1:
                        authorship += ", & "
                    else:
                        authorship += ", "
                authorship += ", ".join([piece for piece in [author.get(
                    "family"), author.get("given")] if piece is not None])
        log(f"Loading citation for {source_identifier}, path: {citation_json_path}")
        pieces = [
          authorship,
          f"({c['issued']['date-parts'][0][0]})",
          c.get("title"),
          c.get("container-title"),
          c.get("publisher"),
          c.get("URL")
        ]
        citation = ". ".join([piece for piece in pieces if piece])
        citation = re.sub(r"\.+", ".", citation)
        log(f"Loading citation for {source_identifier}: {citation}")
        run_sql(
            f"INSERT INTO {citations_table_name} VALUES (%s, %s)",
            interpolations=(source_identifier, citation))
