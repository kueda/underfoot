import jszip from 'jszip';
import { PackBoundingBox, PackMetadata, UnzippedPackData } from './types';

// Namespaces packs loaded from a local file so their ids can never collide
// with a manifest pack's id (which would silently overwrite it in packStore
// and confuse the "update available" check against manifest.updatedAt).
const LOCAL_ID_PREFIX = 'local:';

export class Pack {
  admin1: string;
  admin2: string;
  bbox: PackBoundingBox;
  data?: UnzippedPackData;
  description: string;
  downloadedAt?: string;
  id: string;
  name: string;
  path?: string;
  pmtilesPath?: string;
  // Name of the file a pack was loaded from, for packs loaded from a local
  // file. Browsers don't reveal the rest of the path.
  sourceFileName?: string;
  updatedAt: string;

  constructor(metadata: PackMetadata, data?: UnzippedPackData) {
    this.admin1 = metadata.admin1;
    this.admin2 = metadata.admin2;
    this.bbox = metadata.bbox;
    this.description = metadata.description;
    this.id = metadata.id;
    this.name = metadata.name;
    this.pmtilesPath = metadata.pmtiles_path;
    this.updatedAt = metadata.updated_at;
    this.data = data;
  }

  static fromPack(pack: Pack): Pack {
    const newPack = new Pack(
      {
        admin1: pack.admin1,
        admin2: pack.admin2,
        bbox: pack.bbox,
        description: pack.description,
        id: pack.id,
        name: pack.name,
        path: pack.path,
        pmtiles_path: pack.pmtilesPath,
        updated_at: pack.updatedAt,
      },
      pack.data,
    );
    newPack.downloadedAt = pack.downloadedAt;
    newPack.sourceFileName = pack.sourceFileName;
    return newPack;
  }

  // Derives placeholder metadata for a pack loaded from a local file, since
  // there's no manifest entry to supply a name/id. description, bbox, and
  // admin1/2 aren't read anywhere in the app today, so they're just
  // zeroed/blanked out.
  static metadataFromFileName(fileName: string): PackMetadata {
    const base = fileName.replace(/\.pmtiles\.zip$/i, '').replace(/\.zip$/i, '');
    const slug = base
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'pack';
    return {
      admin1: '',
      admin2: '',
      bbox: {
        bottom: 0, left: 0, right: 0, top: 0,
      },
      description: '',
      id: `${LOCAL_ID_PREFIX}${slug}`,
      name: slug,
      updated_at: new Date().toISOString(),
    };
  }

  // Unzips a freshly-downloaded pack archive once, up front, so later reads
  // (including on subsequent page loads) are plain blob lookups instead of
  // repeating the CPU-bound jszip decompression of the whole archive.
  static async fromZip(metadata: PackMetadata, zipBlob: Blob): Promise<Pack> {
    const { data } = await Pack.unzip(zipBlob);
    return new Pack(metadata, data);
  }

  // Like fromZip for a pack loaded from a local file, where there's no
  // manifest entry to supply metadata. The zip's own pack.json (written by
  // `packs.py`) supplies the title and description; any that's missing, like
  // in zips built before pack.json existed, falls back to the placeholders
  // derived from the file name. The id stays the namespaced local one
  // whatever pack.json says. The file name is kept so the packs list can show
  // where the pack came from.
  static async fromLocalZip(fileName: string, zipBlob: Blob): Promise<Pack> {
    const { data, packJson } = await Pack.unzip(zipBlob);
    const metadata = Pack.metadataFromFileName(fileName);
    const pack = new Pack({
      ...metadata,
      description: packJson?.description || metadata.description,
      name: packJson?.name || metadata.name,
    }, data);
    pack.sourceFileName = fileName;
    return pack;
  }

  private static async unzip(zipBlob: Blob): Promise<{
    data: UnzippedPackData;
    packJson?: Partial<Pick<PackMetadata, 'description' | 'name'>>;
  }> {
    const zip = await jszip.loadAsync(zipBlob);
    const unzipped: UnzippedPackData = {};
    let packJson;
    const zipPaths: string[] = [];
    // Some zip writers (e.g. Python's shutil.make_archive) emit an explicit
    // entry for the top-level directory itself; skip it rather than treat it
    // as a missing file below.
    zip.forEach((path, file) => {
      if (!file.dir) zipPaths.push(path);
    });
    await Promise.all(zipPaths.map(async path => {
      const fname = path.split('/').pop();
      if (!fname || !zip.file(path) || zip.file(path)?.dir) {
        throw new Error(`Zip does not contain file: ${path}`);
      }
      const data = await zip.file(path)?.async('blob');
      if (!data) {
        throw new Error(`Path exists but is empty: ${path}`);
      }
      switch (fname) {
        case 'rocks.pmtiles':
          unzipped.rocks_pmtiles = data;
          break;
        case 'water.pmtiles':
          unzipped.water_pmtiles = data;
          break;
        case 'ways.pmtiles':
          unzipped.ways_pmtiles = data;
          break;
        case 'contours.pmtiles':
          unzipped.contours_pmtiles = data;
          break;
        case 'context.pmtiles':
          unzipped.context_pmtiles = data;
          break;
        case 'rocks-citations.csv':
          unzipped.rocks_citations_csv = data;
          break;
        case 'rocks-rock_units_attrs.csv':
          unzipped.rocks_units_csv = data;
          break;
        case 'water-citations.csv':
          unzipped.water_citations_csv = data;
          break;
        case 'pack.json':
          packJson = Pack.parsePackJson(await data.text());
          break;
      }
    }));
    return { data: unzipped, packJson };
  }

  // Reads just the fields a local pack can use out of a pack.json, ignoring
  // anything that isn't a non-empty string so a malformed file can't blank out
  // the title.
  private static parsePackJson(text: string) {
    let parsed: Record<string, unknown> | null;
    try {
      parsed = JSON.parse(text) as Record<string, unknown> | null;
    }
    catch {
      return undefined;
    }
    const stringOrUndefined = (value: unknown) => (
      typeof value === 'string' && value ? value : undefined
    );
    return {
      description: stringOrUndefined(parsed?.description),
      name: stringOrUndefined(parsed?.name),
    };
  }

  unzippedData(): Promise<UnzippedPackData> {
    if (!this.data) throw new Error('No unzipped data available');
    return Promise.resolve(this.data);
  }
}
