import { Manifest } from './Manifest';
import { Pack } from './Pack';

export interface PackMetadata {
  admin1: string;
  admin2: string;
  bbox: PackBoundingBox;
  description: string;
  id: string;
  name: string;
  path?: string;
  pmtiles_path?: string;
  updated_at: string;
}

export interface RemoteManifest {
  packs: PackMetadata[];
  updated_at: string;
}

export interface PackBoundingBox {
  bottom: number;
  left: number;
  right: number;
  top: number;
}

export interface UnderfootFeature {
  citation?: string;
  id: number;
  source: string;
  title?: string;
}

export interface RockUnit extends UnderfootFeature {
  code: string;
  controlled_span?: string;
  description?: string;
  est_age?: number;
  formation?: string;
  grouping?: string;
  lithology: string;
  max_age?: number;
  min_age?: string;
  rock_type?: string;
  span?: string;
  title?: string;
}

export interface WaterFeature extends UnderfootFeature {
  layer: string;
}

export type UnzippedPackData = {
  context_pmtiles?: Blob;
  contours_pmtiles?: Blob;
  rocks_citations_csv?: Blob;
  rocks_units_csv?: Blob;
  rocks_pmtiles?: Blob;
  water_citations_csv?: Blob;
  water_waterways_network_csv?: Blob;
  water_pmtiles?: Blob;
  ways_pmtiles?: Blob;
};

export interface DownloadOptions {
  onProgress?: (value: { loadedBytes: number; totalBytes: number }) => void;
  signal?: AbortSignal;
}

export interface PackStore {
  addFromFile: (file: File) => Promise<string>;
  currentPackId: string | null;
  download: (packId: string, options?: DownloadOptions) => Promise<void>;
  get: (packId: string) => Promise<Pack | undefined>;
  getCurrentPackId: () => Promise<string | null>;
  list: () => Promise<Pack[]>;
  listLocal: () => Promise<Pack[]>;
  manifest: Manifest | undefined;
  remove: (packId: string) => Promise<void>;
  setCurrent: (packId: string | null) => void;
  error: Error | null;
}
