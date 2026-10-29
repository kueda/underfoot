# pylint: disable=missing-function-docstring
"""Tests for sources util"""

import http.server
import pathlib
import subprocess
import threading

import pytest

from sources import util

# download_file retries every error, which makes tests of permanent failures wait through
# all of curl's retries, and curl uses the last --retry it's given
NO_RETRIES = ["--retry", "0"]

def test_extless_basename_removes_extension():
    path = str(pathlib.Path(__file__))
    assert path.endswith(".py")
    assert util.extless_basename(path) == "test_util"


def test_download_file_writes_file_to_path(tmp_path):
    src = tmp_path / "src.zip"
    src.write_bytes(b"zip contents")
    dest = tmp_path / "dest.zip"
    assert util.download_file(src.as_uri(), str(dest)) == str(dest)
    assert dest.read_bytes() == b"zip contents"
    assert sorted(tmp_path.iterdir()) == sorted([src, dest])


def test_download_file_defaults_to_url_basename_in_cwd(tmp_path, monkeypatch):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src = src_dir / "archive.zip"
    src.write_bytes(b"zip contents")
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.chdir(work_dir)
    assert util.download_file(src.as_uri()) == "archive.zip"
    assert (work_dir / "archive.zip").read_bytes() == b"zip contents"


def test_download_file_leaves_nothing_at_path_when_download_fails(tmp_path):
    """Regression test: curl used to write straight to the final path, so an
    interrupted download left a partial file there, and later runs skipped
    the download because the file existed"""
    dest = tmp_path / "dest.zip"
    with pytest.raises(subprocess.CalledProcessError):
        util.download_file(
            (tmp_path / "missing.zip").as_uri(), str(dest), curl_args=NO_RETRIES
        )
    assert not dest.exists()


def test_download_file_fails_on_http_error(tmp_path):
    """Without --fail, curl exits 0 and saves the error page as the download"""
    class NotFoundHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # pylint: disable=invalid-name
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")

        def log_message(self, *args):  # pylint: disable=arguments-differ
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), NotFoundHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dest = tmp_path / "dest.zip"
        with pytest.raises(subprocess.CalledProcessError):
            util.download_file(
                f"http://127.0.0.1:{server.server_port}/x.zip", str(dest), curl_args=NO_RETRIES
            )
        assert not dest.exists()
    finally:
        server.shutdown()


def test_download_file_retries_when_rate_limited(tmp_path):
    """Regression test: Census servers answer bursts of requests with 429 Too
    Many Requests, and one of those used to fail a whole pack build"""
    requests = []

    class RateLimitOnceHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # pylint: disable=invalid-name
            requests.append(self.path)
            if len(requests) == 1:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(b"slow down")
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"zip contents")

        def log_message(self, *args):  # pylint: disable=arguments-differ
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), RateLimitOnceHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dest = tmp_path / "dest.zip"
        util.download_file(f"http://127.0.0.1:{server.server_port}/x.zip", str(dest))
        assert dest.read_bytes() == b"zip contents"
        assert len(requests) == 2
    finally:
        server.shutdown()


def test_download_file_retries_when_the_connection_breaks(tmp_path):
    """Regression test: curl only retries errors it considers transient, so a
    ScienceBase download that broke off with "HTTP/2 stream 1 was not closed
    cleanly" failed a whole pack build"""
    requests = []

    class BreakOnceHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # pylint: disable=invalid-name
            requests.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", "12")
            self.end_headers()
            if len(requests) == 1:
                # Close the connection partway through the body
                self.wfile.write(b"zip")
                self.close_connection = True  # pylint: disable=attribute-defined-outside-init
                return
            self.wfile.write(b"zip contents")

        def log_message(self, *args):  # pylint: disable=arguments-differ
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), BreakOnceHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dest = tmp_path / "dest.zip"
        util.download_file(f"http://127.0.0.1:{server.server_port}/x.zip", str(dest))
        assert dest.read_bytes() == b"zip contents"
        assert len(requests) == 2
    finally:
        server.shutdown()


def test_make_work_dir_tolerates_another_process_making_it_first(tmp_path, monkeypatch):
    # Parallel water workers share work-tiger_water, so another one can make it
    # between this one's check and its makedirs
    util_dir = tmp_path / "util"
    util_dir.mkdir()
    monkeypatch.setattr(util, "__file__", str(util_dir / "__init__.py"))
    isdir = util.os.path.isdir
    checks = []

    def isdir_made_by_another_worker_after_first_check(path):
        checks.append(path)
        if len(checks) == 1:
            (tmp_path / "work-tiger_water").mkdir()
            return False
        return isdir(path)

    monkeypatch.setattr(util.os.path, "isdir", isdir_made_by_another_worker_after_first_check)
    work_path = util.make_work_dir("tiger_water.py")
    assert pathlib.Path(work_path).resolve() == tmp_path / "work-tiger_water"
