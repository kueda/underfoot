from . import make_work_dir, run_sql, log
from psycopg2.errors import UndefinedTable
import json
import os
import re

# TODO if this is going to get reused for other citations it should probably be
# in packs.py
CITATIONS_TABLE_NAME = "citations"


def create_table():
    """Create the citations table in the database"""
    run_sql(f"""
      CREATE TABLE IF NOT EXISTS {CITATIONS_TABLE_NAME} (
        source VARCHAR(255),
        citation TEXT)
    """)


def load_citation_for_source(source_identifier):
    """Reads citation info from JSON file for source and inserts it into the database"""
    # Delete existing row, create table if missing
    try:
        run_sql(f"""
            DELETE FROM {CITATIONS_TABLE_NAME}
            WHERE source = '{source_identifier}'
        """)
    except UndefinedTable:
        create_table()
    path = os.path.join("sources", f"{source_identifier}.py")
    work_path = make_work_dir(path)
    citation_json_path = os.path.join(work_path, "citation.json")
    if not os.path.isfile(citation_json_path):
        return
    with open(citation_json_path, encoding="utf-8") as citation_file:
        citation = json.loads(citation_file.read())[0]
        authorship = None
        if "author" in citation:
            authorship = ""
            for idx, author in enumerate(citation["author"]):
                if idx != 0:
                    if idx == len(citation["author"]) - 1:
                        authorship += ", & "
                    else:
                        authorship += ", "
                authorship += ", ".join([piece for piece in [author.get(
                    "family"), author.get("given")] if piece is not None])
        log(f"Loading citation for {source_identifier}, path: {citation_json_path}")
        pieces = [
          authorship,
          f"({citation['issued']['date-parts'][0][0]})",
          citation.get("title"),
          citation.get("container-title"),
          citation.get("publisher"),
          citation.get("URL")
        ]
        citation_txt = ". ".join([piece for piece in pieces if piece])
        citation_txt = re.sub(r"\.+", ".", citation_txt)
        log(f"Loading citation for {source_identifier}: {citation_txt}")
        run_sql(
            f"INSERT INTO {CITATIONS_TABLE_NAME} VALUES (%s, %s)",
            interpolations=(source_identifier, citation_txt))
