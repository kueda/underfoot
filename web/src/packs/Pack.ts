import { readPackFiles } from './packFiles';
import {
  ExtractedPack, PackBoundingBox, PackCenter, PackMetadata, UnzippedPackData,
} from './types';

// Namespaces packs loaded from a local file so their ids can never collide
// with a manifest pack's id (which would silently overwrite it in packStore
// and confuse the "update available" check against manifest.updatedAt).
export const LOCAL_ID_PREFIX = 'local:';

export class Pack {
  admin1: string;
  admin2: string;
  bbox: PackBoundingBox;
  // Older versions of the app stored a pack's unzipped files here, in
  // IndexedDB. Packs downloaded since keep them in the folder at filesDir.
  data?: UnzippedPackData;
  defaultCenter?: PackCenter;
  defaultZoom?: number;
  description: string;
  downloadedAt?: string;
  // Where the pack's unzipped files are in the origin private file system,
  // relative to the packs folder (see packFiles.ts)
  filesDir?: string;
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
    this.defaultCenter = metadata.default_center;
    this.defaultZoom = metadata.default_zoom;
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
        default_center: pack.defaultCenter,
        default_zoom: pack.defaultZoom,
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
    newPack.filesDir = pack.filesDir;
    newPack.sourceFileName = pack.sourceFileName;
    return newPack;
  }

  // Derives placeholder metadata for a pack loaded from a local file, since
  // there's no manifest entry to supply a name/id. description isn't read
  // anywhere in the app today, and admin1/2 only name the groups in the "All"
  // tab, which lists local packs ungrouped (when offline), so they're just
  // blanked out. The zeroed bbox means "Near me" in the packs dialog never
  // lists these packs.
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

  // Makes a pack from a freshly-downloaded pack archive that was unzipped
  // once, up front, so later reads (including on subsequent page loads) are
  // plain file lookups instead of repeating the decompression of the whole
  // archive. The default view comes from the zip's pack.json, since that's
  // what the pack was built with.
  static fromExtracted(metadata: PackMetadata, extracted: ExtractedPack): Pack {
    const packJson = Pack.parsePackJson(extracted.packJson);
    const pack = new Pack({
      ...metadata,
      default_center: packJson?.default_center,
      default_zoom: packJson?.default_zoom,
    });
    pack.filesDir = extracted.filesDir;
    return pack;
  }

  // Like fromExtracted for a pack loaded from a local file, where there's no
  // manifest entry to supply metadata. The zip's own pack.json (written by
  // `packs.py`) supplies the title and description; any that's missing, like
  // in zips built before pack.json existed, falls back to the placeholders
  // derived from the file name. The id stays the namespaced local one
  // whatever pack.json says. The file name is kept so the packs list can show
  // where the pack came from.
  static fromLocalExtracted(fileName: string, extracted: ExtractedPack): Pack {
    const packJson = Pack.parsePackJson(extracted.packJson);
    const metadata = Pack.metadataFromFileName(fileName);
    const pack = new Pack({
      ...metadata,
      default_center: packJson?.default_center,
      default_zoom: packJson?.default_zoom,
      description: packJson?.description || metadata.description,
      name: packJson?.name || metadata.name,
    });
    pack.filesDir = extracted.filesDir;
    pack.sourceFileName = fileName;
    return pack;
  }

  // Reads just the fields a pack can use out of a pack.json, ignoring anything
  // that isn't a non-empty string so a malformed file can't blank out the
  // title, and any default view that isn't made of numbers.
  private static parsePackJson(text?: string) {
    if (text === undefined) return undefined;
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
    const numberOrUndefined = (value: unknown) => (
      typeof value === 'number' && Number.isFinite(value) ? value : undefined
    );
    const center = parsed?.default_center as Record<string, unknown> | null | undefined;
    const lat = numberOrUndefined(center?.lat);
    const lon = numberOrUndefined(center?.lon);
    return {
      default_center: lat !== undefined && lon !== undefined ? { lat, lon } : undefined,
      default_zoom: numberOrUndefined(parsed?.default_zoom),
      description: stringOrUndefined(parsed?.description),
      name: stringOrUndefined(parsed?.name),
    };
  }

  isDownloaded(): boolean {
    return !!(this.filesDir || this.data);
  }

  unzippedData(): Promise<UnzippedPackData> {
    if (this.filesDir) return readPackFiles(this.filesDir);
    if (!this.data) throw new Error('No unzipped data available');
    return Promise.resolve(this.data);
  }
}
