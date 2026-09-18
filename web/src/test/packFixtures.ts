import jszip from 'jszip';
import { vi } from 'vitest';
import { PackMetadata, RemoteManifest } from '../packs/types';

const STATIC_HOST = 'https://static.underfoot.rocks';

// Pack downloads are sent in this many chunks so progress reporting has something to report
export const DOWNLOAD_CHUNKS = 3;

export function packMetadata(overrides: Partial<PackMetadata> = {}): PackMetadata {
  return {
    admin1: 'California',
    admin2: 'Alameda County',
    bbox: {
      bottom: 37.7, left: -122.35, right: -122.1, top: 37.9,
    },
    description: 'Oakland, Berkeley, and the East Bay hills',
    id: 'us-ca-oakland',
    name: 'Oakland',
    pmtiles_path: 'us-ca-oakland.pmtiles.zip',
    updated_at: '2026-01-01T00:00:00.000Z',
    ...overrides,
  };
}

export function packZip(files: Record<string, string>): Promise<Blob> {
  const zip = new jszip();
  Object.entries(files).forEach(([path, contents]) => zip.file(path, contents));
  return zip.generateAsync({ type: 'blob' });
}

// Stands in for static.underfoot.rocks. Tests can change it between requests, e.g. to publish
// an update or to go offline.
export interface StaticServer {
  files: Record<string, Blob>;
  // Leave undefined to make the manifest request fail like it would offline
  manifest?: RemoteManifest;
  sendContentLength: boolean;
}

export function stubStaticServer() {
  const server: StaticServer = { files: {}, sendContentLength: true };
  const fetchMock = vi.fn(async (url: string): Promise<Response> => {
    if (url === `${STATIC_HOST}/manifest.json`) {
      if (!server.manifest) throw new TypeError('Failed to fetch');
      return Response.json(server.manifest);
    }
    const file = server.files[url.replace(`${STATIC_HOST}/`, '')];
    // Fail with the URL instead of serving a 404, which usePackStore would try to unzip, so a
    // test that requests the wrong URL says which one
    if (!file) throw new Error(`Test server has no file at ${url}`);
    const bytes = new Uint8Array(await file.arrayBuffer());
    const chunkSize = Math.ceil(bytes.length / DOWNLOAD_CHUNKS);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let start = 0; start < bytes.length; start += chunkSize) {
          controller.enqueue(bytes.slice(start, start + chunkSize));
        }
        controller.close();
      },
    });
    const headers = server.sendContentLength
      ? { 'Content-Length': bytes.length.toString() }
      : undefined;
    return new Response(body, { headers });
  });
  vi.stubGlobal('fetch', fetchMock);
  return { fetchMock, server };
}

// Adds a pack to the manifest, replacing any with the same id, and serves a zip for it. The
// zip's rocks.pmtiles contains `rocks`, so tests can tell versions of a pack apart.
export async function publishPack(server: StaticServer, metadata: PackMetadata, rocks = 'rocks') {
  const otherPacks = server.manifest?.packs.filter(pack => pack.id !== metadata.id) || [];
  server.manifest = {
    packs: [...otherPacks, metadata],
    updated_at: metadata.updated_at,
  };
  if (metadata.pmtiles_path) {
    server.files[metadata.pmtiles_path] = await packZip({
      [`${metadata.id}/rocks.pmtiles`]: rocks,
    });
  }
}
