import { LOCAL_ID_PREFIX, Pack } from './Pack';

export interface PackNode {
  // Sorted by label
  children: PackNode[];
  // Number of packs below this node, not counting its own
  descendantPackCount: number;
  id: string;
  label: string;
  parentId: string | null;
  // Absent for a virtual node: a placeholder for an id prefix that has packs under it but isn't
  // a pack itself, like `us-wy` when only `us-wy-tetons` exists
  pack?: Pack;
}

export interface PackTree {
  get: (id: string) => PackNode | undefined;
  roots: PackNode[];
}

// The number of leading id segments that name places rather than a pack: the country and the
// state, which admin1 and admin2 hold. Hyphens past them are just part of a pack's name, like
// `central-coast` in `us-ca-central-coast`.
const PLACE_LEVELS = 2;

// "1 pack", "2 packs"
export const packCountLabel = (count: number) => `${count} ${count === 1 ? 'pack' : 'packs'}`;

// Packs nest by their ids: `us-ca-oakland` is under `us-ca`, which is under `us`. A pack's
// parent is the longest prefix of its id that has a node. Packs get nodes, and so do the country
// and state of every pack, so a pack still has a place when the packs above it don't exist.
// Nothing deeper gets a node unless it's a pack. Packs loaded from a local file, which the list
// falls back to when offline, have ids that say nothing about places, so they stay at the top.
export function buildPackTree(packs: Pack[]): PackTree {
  const nodes = new Map<string, PackNode>();
  const virtualLabels = new Map<string, string>();

  const ensureNode = (id: string) => {
    let node = nodes.get(id);
    if (!node) {
      node = {
        children: [],
        descendantPackCount: 0,
        id,
        label: '',
        parentId: null,
      };
      nodes.set(id, node);
    }
    return node;
  };

  const isLocal = (id: string) => id.startsWith(LOCAL_ID_PREFIX);

  packs.forEach(pack => {
    if (isLocal(pack.id)) {
      ensureNode(pack.id).pack = pack;
      return;
    }
    const segments = pack.id.split('-');
    segments.slice(0, PLACE_LEVELS).forEach((_segment, index) => {
      const id = segments.slice(0, index + 1).join('-');
      ensureNode(id);
      const metadataLabel = [pack.admin1, pack.admin2][index];
      if (metadataLabel && !virtualLabels.has(id)) virtualLabels.set(id, metadataLabel);
    });
    ensureNode(pack.id).pack = pack;
  });

  nodes.forEach(node => {
    const segments = node.id.split('-');
    const maxLength = isLocal(node.id) ? 0 : segments.length - 1;
    for (let length = maxLength; length > 0; length -= 1) {
      const candidate = segments.slice(0, length).join('-');
      if (nodes.has(candidate)) {
        node.parentId = candidate;
        break;
      }
    }
    node.label = node.pack?.name ?? virtualLabels.get(node.id) ?? segments[segments.length - 1];
  });
  nodes.forEach(node => {
    const parent = node.parentId ? nodes.get(node.parentId) : undefined;
    if (parent) parent.children.push(node);
  });
  const byLabel = (a: PackNode, b: PackNode) => a.label.localeCompare(b.label);
  const countPacks = (node: PackNode): number => {
    node.children.sort(byLabel);
    node.descendantPackCount = node.children.reduce(
      (sum, child) => sum + countPacks(child) + (child.pack ? 1 : 0),
      0,
    );
    return node.descendantPackCount;
  };
  const roots = [...nodes.values()].filter(node => !node.parentId).sort(byLabel);
  roots.forEach(countPacks);

  return { get: id => nodes.get(id), roots };
}
