from . import make_work_dir, run_sql, run_sql_with_retries, log
import json
import os
import re

# TODO if this is going to get reused for other citations it should probably be
# in packs.py
CITATIONS_TABLE_NAME = "citations"


def create_table():
    """Create the citations table in the database"""
    run_sql_with_retries(f"""
      CREATE TABLE IF NOT EXISTS {CITATIONS_TABLE_NAME} (
        source VARCHAR(255),
        citation TEXT)
    """)


def citation_txt_from_csl_json_path(citation_json_path):
    """Return a string citation for a path to a CSL JSON file"""
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
        pieces = [
          authorship,
          f"({citation['issued']['date-parts'][0][0]})",
          citation.get("title"),
          citation.get("container-title"),
          citation.get("publisher"),
          citation.get("URL")
        ]
        citation_txt = ". ".join([piece for piece in pieces if piece])
        return re.sub(r"\.+", ".", citation_txt)


def find_citation_json_path(source_identifier):
    """Path to the CSL JSON file for a source, or None if it has none

    Prefers the copy in the source's work dir, which may have been generated
    during the build, and falls back to the one checked in with the source.
    Cached sources skip the build, so their work dir may not have a copy.
    """
    path = os.path.join("sources", f"{source_identifier}.py")
    work_path = make_work_dir(path)
    sources_path = os.path.dirname(work_path)
    for dir_path in [work_path, os.path.join(sources_path, source_identifier)]:
        citation_json_path = os.path.join(dir_path, "citation.json")
        if os.path.isfile(citation_json_path):
            return citation_json_path
    return None


def load_citation_for_source(source_identifier):
    """Reads citation info from JSON file for source and inserts it into the database"""
    create_table()
    citation_json_path = find_citation_json_path(source_identifier)
    # Only replace the existing row if there's a citation to replace it with
    if not citation_json_path:
        return
    run_sql(f"""
        DELETE FROM {CITATIONS_TABLE_NAME}
        WHERE source = '{source_identifier}'
    """)
    log(f"Loading citation for {source_identifier}, path: {citation_json_path}")
    citation_txt = citation_txt_from_csl_json_path(citation_json_path)
    log(f"Loading citation for {source_identifier}: {citation_txt}")
    run_sql(
        f"INSERT INTO {CITATIONS_TABLE_NAME} VALUES (%s, %s)",
        interpolations=(source_identifier, citation_txt))
