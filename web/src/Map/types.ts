import { RockUnit } from '../packs/types';

export interface UnderfootFeatures {
  [id: number]: RockUnit;
}

export interface Citation {
  source: string;
  citation: string;
}

export interface Citations {
  [source: string]: string;
}
