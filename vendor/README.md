# vendor/

Third-party source that the Docker image build compiles, checked in so image
builds have no external network dependency for it.

## e00compr-1.0.1.tar.gz

The Compressed E00 Read/Write library, used to build the `e00conv` binary that
decompresses ArcInfo `.e00` interchange files for some geologic sources (see
`sources/util/rocks/__init__.py`).

Upstream: http://avce00.maptools.org/dl/e00compr-1.0.1.tar.gz (plain HTTP, and a
historically flaky host, which is why it is vendored here).

SHA-256: `b4a2f582ba0829834a8d40126071e6695f6a30e769131df6924ab3c09728b884`
