import { describe, expect, it } from 'vitest';
import { packMetadata } from '../test/packFixtures';
import { Pack } from './Pack';
import { buildPackTree } from './packTree';

function pack(id: string, overrides = {}) {
  return new Pack(packMetadata({
    admin1: 'United States',
    admin2: 'California',
    id,
    name: id.toUpperCase(),
    ...overrides,
  }));
}

describe('buildPackTree', () => {
  it('nests packs under the pack whose id is their prefix', () => {
    const tree = buildPackTree([pack('us-ca-sfba'), pack('us-ca'), pack('us-ca-oakland')]);
    const state = tree.get('us-ca');
    expect(state?.pack?.id).toBe('us-ca');
    expect(state?.children.map(child => child.id)).toEqual(['us-ca-oakland', 'us-ca-sfba']);
    expect(state?.descendantPackCount).toBe(2);
  });

  it('adds virtual nodes named by admin1 and admin2 where no pack fills the gap', () => {
    const tree = buildPackTree([pack('us-wy-tetons', { admin2: 'Wyoming' })]);
    expect(tree.roots.map(root => [root.id, root.label, !!root.pack]))
      .toEqual([['us', 'United States', false]]);
    const state = tree.get('us-wy');
    expect(state?.label).toBe('Wyoming');
    expect(state?.pack).toBeUndefined();
    expect(state?.children.map(child => child.id)).toEqual(['us-wy-tetons']);
    expect(tree.get('us')?.descendantPackCount).toBe(1);
  });

  it('treats hyphens past the state as part of the name, not as levels', () => {
    const tree = buildPackTree([pack('us-ca'), pack('us-ca-central-coast')]);
    expect(tree.get('us-ca-central')).toBeUndefined();
    expect(tree.get('us-ca-central-coast')?.parentId).toBe('us-ca');
    expect(tree.get('us-ca')?.children.map(child => child.id)).toEqual(['us-ca-central-coast']);
  });

  it('nests under the state when the packs between it and a deeper pack are missing', () => {
    const tree = buildPackTree([pack('us-wy-tetons-south')]);
    expect(tree.get('us-wy-tetons')).toBeUndefined();
    expect(tree.get('us-wy-tetons-south')?.parentId).toBe('us-wy');
    expect(tree.get('us-wy')?.label).toBe('California');
  });

  it('nests under the nearest pack that exists', () => {
    const tree = buildPackTree([
      pack('us-ca'),
      pack('us-ca-sfba'),
      pack('us-ca-sfba-east'),
    ]);
    expect(tree.get('us-ca-sfba-east')?.parentId).toBe('us-ca-sfba');
  });

  it('turns a virtual node into a real one once its pack exists', () => {
    const tree = buildPackTree([pack('us-wy-tetons'), pack('us-wy', { name: 'Wyoming (state)' })]);
    expect(tree.get('us-wy')?.pack?.name).toBe('Wyoming (state)');
    expect(tree.get('us-wy')?.label).toBe('Wyoming (state)');
  });

  it('makes a one-segment pack a root next to other countries', () => {
    const tree = buildPackTree([
      pack('us-ca'),
      pack('cr', { admin1: 'Costa Rica', admin2: '', name: 'Costa Rica' }),
    ]);
    expect(tree.roots.map(root => root.label)).toEqual(['Costa Rica', 'United States']);
    expect(tree.get('cr')?.pack).toBeDefined();
  });

  it('names a virtual node from the first pack that has a name for it', () => {
    const tree = buildPackTree([
      pack('us-ca-a', { admin2: '' }),
      pack('us-ca-b', { admin2: 'California' }),
    ]);
    expect(tree.get('us-ca')?.label).toBe('California');
  });

  it('keeps packs loaded from a local file at the top level, ungrouped', () => {
    const tree = buildPackTree([
      pack('local:darwin-hills', { admin1: '', admin2: '' }),
      pack('local:darwin', { admin1: '', admin2: '' }),
    ]);
    expect(tree.roots.map(root => root.id)).toEqual(['local:darwin', 'local:darwin-hills']);
    expect(tree.get('local:darwin-hills')?.parentId).toBeNull();
    expect(tree.get('local')).toBeUndefined();
  });

  it('is empty without packs', () => {
    expect(buildPackTree([]).roots).toEqual([]);
  });
});
