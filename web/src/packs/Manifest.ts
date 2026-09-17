import { Pack } from './Pack';
import { RemoteManifest } from './types';

export class Manifest {
  packs: Pack[];
  updatedAt: Date;

  constructor(json: RemoteManifest) {
    this.packs = json.packs.map(remotePack => new Pack(remotePack));
    this.updatedAt = new Date(json.updated_at);
  }
}
