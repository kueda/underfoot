# pylint: disable=missing-function-docstring
"""Tests for sources util"""

import http.server
import pathlib
import subprocess
import threading

import pytest

from sources import util

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
        util.download_file((tmp_path / "missing.zip").as_uri(), str(dest))
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
            util.download_file(f"http://127.0.0.1:{server.server_port}/x.zip", str(dest))
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
