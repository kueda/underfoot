import { UnderfootFeature } from '../packs/types';

export interface UnderfootFeatures {
  [id: number]: UnderfootFeature;
}

export interface Citation {
  source: string;
  citation: string;
}

export interface Citations {
  [source: string]: string;
}
