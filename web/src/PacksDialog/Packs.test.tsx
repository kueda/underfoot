import { act, fireEvent, renderHook, render, screen, waitFor, within } from '@testing-library/react';
import localforage from 'localforage';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  packMetadata, packZip, publishPack, StaticServer, stubStaticServer,
} from '../test/packFixtures';
import { useHidePacksModal, useRequestPack } from '../useAppStore';
import Packs from './Packs';

const BERKELEY_PATH = 'us-ca-berkeley.pmtiles.zip';

// The same IndexedDB stores usePackStore uses, for clearing between tests
const packStorage = localforage.createInstance({ name: 'packStore' });
const prefStorage = localforage.createInstance({ name: 'prefStore' });

function row(name: string) {
  const item = screen.getByText(name).closest('li');
  if (!item) throw new Error(`No list item for ${name}`);
  return within(item);
}

// Opens the virtual United States and California levels the test packs sit under
async function openCalifornia() {
  fireEvent.click(await screen.findByText('United States'));
  fireEvent.click(await screen.findByText('California'));
}

// Makes the Berkeley download stall after its first half, so a test can start it, do other
// things, and then let it finish
function stallBerkeleyDownload(
  fetchMock: ReturnType<typeof stubStaticServer>['fetchMock'],
  server: StaticServer,
) {
  const serveEverything = fetchMock.getMockImplementation();
  if (!serveEverything) throw new Error('fetch is not stubbed');
  let finish = () => {};
  fetchMock.mockImplementation(async (url: string) => {
    if (!url.endsWith(BERKELEY_PATH)) return serveEverything(url);
    const bytes = new Uint8Array(await server.files[BERKELEY_PATH].arrayBuffer());
    const half = Math.ceil(bytes.length / 2);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes.slice(0, half));
        finish = () => {
          controller.enqueue(bytes.slice(half));
          controller.close();
        };
      },
    });
    return new Response(body, { headers: { 'Content-Length': bytes.length.toString() } });
  });
  return () => finish();
}

describe('Packs', () => {
  let fetchMock: ReturnType<typeof stubStaticServer>['fetchMock'];
  let server: StaticServer;

  beforeEach(async () => {
    await Promise.all([packStorage.clear(), prefStorage.clear()]);
    ({ fetchMock, server } = stubStaticServer());
    await publishPack(server, packMetadata({ description: 'The East Bay' }));
    await publishPack(server, packMetadata({
      description: 'Across the bay',
      id: 'us-ca-berkeley',
      name: 'Berkeley',
      pmtiles_path: BERKELEY_PATH,
    }));
  });

  it('does not nest a paragraph inside a paragraph, which is invalid HTML', async () => {
    const { container } = render(<Packs />);
    await screen.findByText('United States');
    expect(screen.getByText(/Download map data for use offline/)).toBeTruthy();
    expect(container.querySelector('p p')).toBeNull();
  });

  describe('when a pack is loaded from a local file', () => {
    it('shows the pack\'s own title and description and the file it came from', async () => {
      const zip = await packZip({
        'us-ca-oakland.pmtiles/pack.json': JSON.stringify(packMetadata({
          description: 'Oakland, CA, USA. Mostly for testing some place small.',
          name: 'Oakland, CA, USA',
        })),
        'us-ca-oakland.pmtiles/rocks.pmtiles': 'rocks',
      });
      const { container } = render(<Packs />);
      fireEvent.click(await screen.findByRole('tab', { name: 'Downloaded' }));
      const input = container.querySelector('input[type="file"]');
      if (!input) throw new Error('No file input');
      fireEvent.change(input, {
        target: { files: [new File([zip], 'us-ca-oakland.pmtiles.zip')] },
      });

      const item = await screen.findByText('Oakland, CA, USA');
      const localRow = within(item.closest('li') as HTMLElement);
      expect(localRow.getByText(/Mostly for testing some place small/)).toBeTruthy();
      expect(localRow.getByText('Loaded from us-ca-oakland.pmtiles.zip')).toBeTruthy();
    });

    it('shows just the file it came from when the zip has no pack.json', async () => {
      const zip = await packZip({ 'us-ca-oakland.pmtiles/rocks.pmtiles': 'rocks' });
      const { container } = render(<Packs />);
      fireEvent.click(await screen.findByRole('tab', { name: 'Downloaded' }));
      const input = container.querySelector('input[type="file"]');
      if (!input) throw new Error('No file input');
      fireEvent.change(input, {
        target: { files: [new File([zip], 'us-ca-oakland.pmtiles.zip')] },
      });

      const item = await screen.findByText('us-ca-oakland');
      const localRow = within(item.closest('li') as HTMLElement);
      expect(localRow.getByText('Loaded from us-ca-oakland.pmtiles.zip')).toBeTruthy();
    });
  });

  describe('when a pack download finishes while another is still downloading', () => {
    it('keeps showing the other download in progress', async () => {
      const finishBerkeley = stallBerkeleyDownload(fetchMock, server);
      render(<Packs />);
      await openCalifornia();
      await screen.findByText('Berkeley');

      fireEvent.click(row('Berkeley').getByRole('button', { name: 'download' }));
      await screen.findByText(/^\d+% downloaded/);

      fireEvent.click(row('Oakland').getByRole('button', { name: 'download' }));
      await waitFor(() => {
        expect(row('Oakland').getByRole('button', { name: 'delete' })).toBeTruthy();
      });

      expect(row('Berkeley').getByText(/^\d+% downloaded/)).toBeTruthy();
      expect(row('Berkeley').getByRole('button', { name: 'stop' })).toBeTruthy();

      finishBerkeley();
      await waitFor(() => {
        expect(row('Berkeley').getByRole('button', { name: 'delete' })).toBeTruthy();
      });
    });
  });

  describe('when a pack is deleted while another is still downloading', () => {
    it('keeps showing the other download in progress', async () => {
      const finishBerkeley = stallBerkeleyDownload(fetchMock, server);
      render(<Packs />);
      await openCalifornia();
      await screen.findByText('Oakland');
      fireEvent.click(row('Oakland').getByRole('button', { name: 'download' }));
      await waitFor(() => {
        expect(row('Oakland').getByRole('button', { name: 'delete' })).toBeTruthy();
      });

      fireEvent.click(row('Berkeley').getByRole('button', { name: 'download' }));
      await screen.findByText(/^\d+% downloaded/);

      fireEvent.click(row('Oakland').getByRole('button', { name: 'delete' }));
      await waitFor(() => {
        expect(row('Oakland').getByRole('button', { name: 'download' })).toBeTruthy();
      });

      expect(row('Berkeley').getByText(/^\d+% downloaded/)).toBeTruthy();
      expect(row('Berkeley').getByRole('button', { name: 'stop' })).toBeTruthy();

      finishBerkeley();
      await waitFor(() => {
        expect(row('Berkeley').getByRole('button', { name: 'delete' })).toBeTruthy();
      });
    });
  });

  describe('when packs are nested', () => {
    // Shared-link tests leave a requested pack in the app store, which would otherwise carry over
    // into the next test, including when an assertion fails before the test can clean up
    afterEach(() => {
      const { result: hidePacksModal } = renderHook(() => useHidePacksModal());
      act(() => hidePacksModal.current());
    });

    beforeEach(async () => {
      await publishPack(server, packMetadata({
        description: 'The whole state',
        id: 'us-ca',
        name: 'California (state)',
        pmtiles_path: 'us-ca.pmtiles.zip',
      }));
    });

    it('lists only the top level, with a count of the packs inside', async () => {
      render(<Packs />);
      const country = await screen.findByText('United States');
      expect(within(country.closest('li') as HTMLElement).getByText('3 packs')).toBeTruthy();
      expect(screen.queryByText('California (state)')).toBeNull();
      expect(screen.queryByText('Oakland')).toBeNull();
    });

    it('opens a pack\'s own pack and the packs inside it, and goes back', async () => {
      render(<Packs />);
      fireEvent.click(await screen.findByText('United States'));
      fireEvent.click(await screen.findByRole('button', {
        name: 'Show packs within California (state)',
      }));

      // The state pack is still there to download, above the packs inside it
      expect(await screen.findByText('Oakland')).toBeTruthy();
      expect(screen.getByText('Berkeley')).toBeTruthy();
      // (the name is also in the breadcrumb, so find the row by its description)
      expect(row('The whole state').getByRole('button', { name: 'download' })).toBeTruthy();

      fireEvent.click(screen.getByRole('button', { name: 'Back' }));
      expect(await screen.findByText('California (state)')).toBeTruthy();
      expect(screen.queryByText('Oakland')).toBeNull();

      fireEvent.click(screen.getByRole('button', { name: 'Back' }));
      expect(await screen.findByText('United States')).toBeTruthy();
    });

    it('opens the level a shared link\'s pack is on', async () => {
      const { result: requestPack } = renderHook(() => useRequestPack());
      act(() => requestPack.current('us-ca-oakland'));
      render(<Packs />);
      expect(await screen.findByText('Oakland')).toBeTruthy();
      expect(screen.getByRole('button', { name: 'Back' })).toBeTruthy();
    });

    it('opens the right level for a shared link to a pack with a hyphen in its name', async () => {
      await publishPack(server, packMetadata({
        id: 'us-ca-central-coast',
        name: 'Central Coast',
        pmtiles_path: 'us-ca-central-coast.pmtiles.zip',
      }));
      const { result: requestPack } = renderHook(() => useRequestPack());
      act(() => requestPack.current('us-ca-central-coast'));
      render(<Packs />);
      expect(await screen.findByText('Central Coast')).toBeTruthy();
    });

    it('opens a pack that has packs inside it for a shared link, to show its download button',
      async () => {
        const { result: requestPack } = renderHook(() => useRequestPack());
        act(() => requestPack.current('us-ca'));
        render(<Packs />);

        expect(await screen.findByText('Oakland')).toBeTruthy();
        expect(row('The whole state').getByRole('button', { name: 'download' })).toBeTruthy();
      });

    it('opens a pack that has packs inside it when its row is tapped, with no radio', async () => {
      render(<Packs />);
      fireEvent.click(await screen.findByText('United States'));
      await screen.findByText('California (state)');
      expect(row('California (state)').queryByRole('radio')).toBeNull();

      fireEvent.click(screen.getByText('California (state)'));
      expect(await screen.findByText('Oakland')).toBeTruthy();
    });

    it('leaves a pack that has packs inside it to be downloaded from inside', async () => {
      render(<Packs />);
      fireEvent.click(await screen.findByText('United States'));
      await screen.findByText('California (state)');

      const stateRow = row('California (state)');
      expect(stateRow.getByRole('button', { name: 'Show packs within California (state)' }))
        .toBeTruthy();
      expect(stateRow.queryByRole('button', { name: 'download' })).toBeNull();
    });

    it('shows how many packs are within a pack on its button', async () => {
      render(<Packs />);
      fireEvent.click(await screen.findByText('United States'));
      const button = await screen.findByRole('button', {
        name: 'Show packs within California (state)',
      });
      expect(within(button).getByText('2 packs')).toBeTruthy();
    });

    it('downloads a pack from inside a level', async () => {
      render(<Packs />);
      fireEvent.click(await screen.findByText('United States'));
      fireEvent.click(await screen.findByRole('button', {
        name: 'Show packs within California (state)',
      }));
      await screen.findByText('Oakland');

      fireEvent.click(row('Oakland').getByRole('button', { name: 'download' }));
      await waitFor(() => {
        expect(row('Oakland').getByRole('button', { name: 'delete' })).toBeTruthy();
      });
    });
  });
});
