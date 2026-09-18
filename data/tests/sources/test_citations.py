# pylint: disable=missing-function-docstring
"""Tests citation parsing"""

import json
from pathlib import Path

import pytest

from sources.util import citations
from sources.util.citations import citation_txt_from_csl_json_path

def test_csl_json_files():
    """Test parsability of CSL JSON citations checked in to the repo"""
    for path in Path("sources").rglob("citation.json"):
        if "work-" in str(path):
            continue
        try:
            if citation_txt := citation_txt_from_csl_json_path(path):
                assert citation_txt != ""
        except Exception as parsing_exception:
            print(f"Exception parsing citation for {path}")
            raise parsing_exception


def write_csl_json(dir_path, title):
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "citation.json").write_text(json.dumps([{
        "title": title,
        "author": [{"family": "Graymer", "given": "R.W."}],
        "issued": {"date-parts": [[2000]]}
    }]), encoding="utf-8")


@pytest.fixture(name="citation_env")
def fixture_citation_env(monkeypatch, tmp_path):
    """Fakes the database and lays out tmp_path like data/sources/

    Returns a namespace with the SQL statements that were run, the source
    module dir (tmp_path/"foo"), and the work dir (tmp_path/"work-foo").
    """
    class Env:
        statements = []
        source_dir = tmp_path / "foo"
        work_dir = tmp_path / "work-foo"

    def fake_run_sql(sql, interpolations=None, **_kwargs):
        Env.statements.append((sql.strip(), interpolations))

    def fake_make_work_dir(_path):
        Env.work_dir.mkdir(exist_ok=True)
        return str(Env.work_dir)

    Env.statements = []
    monkeypatch.setattr(citations, "run_sql", fake_run_sql)
    monkeypatch.setattr(citations, "run_sql_with_retries", fake_run_sql)
    monkeypatch.setattr(citations, "make_work_dir", fake_make_work_dir)
    return Env


def statements_starting_with(env, prefix):
    return [s for s in env.statements if s[0].startswith(prefix)]


def test_load_citation_keeps_existing_row_when_there_is_no_citation_file(citation_env):
    citations.load_citation_for_source("foo")
    assert not statements_starting_with(citation_env, "DELETE")
    assert not statements_starting_with(citation_env, "INSERT")


def test_load_citation_ensures_table_exists_when_there_is_no_citation_file(citation_env):
    citations.load_citation_for_source("foo")
    assert statements_starting_with(citation_env, "CREATE TABLE IF NOT EXISTS citations")


def test_load_citation_uses_work_dir_citation(citation_env):
    write_csl_json(citation_env.work_dir, "Work dir title")
    citations.load_citation_for_source("foo")
    assert statements_starting_with(citation_env, "DELETE")
    inserts = statements_starting_with(citation_env, "INSERT")
    assert len(inserts) == 1
    assert inserts[0][1][0] == "foo"
    assert "Work dir title" in inserts[0][1][1]


def test_load_citation_falls_back_to_source_dir_citation(citation_env):
    write_csl_json(citation_env.source_dir, "Source dir title")
    citations.load_citation_for_source("foo")
    assert statements_starting_with(citation_env, "DELETE")
    inserts = statements_starting_with(citation_env, "INSERT")
    assert len(inserts) == 1
    assert inserts[0][1][0] == "foo"
    assert "Source dir title" in inserts[0][1][1]


def test_load_citation_prefers_work_dir_citation_over_source_dir(citation_env):
    write_csl_json(citation_env.work_dir, "Work dir title")
    write_csl_json(citation_env.source_dir, "Source dir title")
    citations.load_citation_for_source("foo")
    inserts = statements_starting_with(citation_env, "INSERT")
    assert len(inserts) == 1
    assert "Work dir title" in inserts[0][1][1]
    assert "Source dir title" not in inserts[0][1][1]
