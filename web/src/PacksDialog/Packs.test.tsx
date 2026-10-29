import { act, fireEvent, renderHook, render, screen, waitFor, within } from '@testing-library/react';
import localforage from 'localforage';
import {
  afterEach, beforeEach, describe, expect, it, vi,
} from 'vitest';
import {
  packMetadata, packZip, publishPack, StaticServer, stubStaticServer,
} from '../test/packFixtures';
import { extractPackInWorker } from '../packs/extractPackInWorker';
import { useHidePacksModal, useRequestPack } from '../useAppStore';
import Packs from './Packs';

// jsdom has no Worker, so unzip packs on the main thread instead
vi.mock('../packs/extractPackInWorker', async () => {
  const { extractPack } = await import('../packs/extractPack');
  return { extractPackInWorker: vi.fn(extractPack) };
});

const BERKELEY_PATH = 'us-ca-berkeley.pmtiles.zip';

const QUOTA_MESSAGE = /not enough storage space/i;

// Makes the next pack unzip fail the way it does when the browser's storage quota runs out
function exceedQuota() {
  vi.mocked(extractPackInWorker).mockRejectedValueOnce(
    new DOMException('The quota has been exceeded.', 'QuotaExceededError'),
  );
}

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
    if (new URL(url).pathname !== `/${BERKELEY_PATH}`) return serveEverything(url);
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
    // restoreMocks only restores spies, so drop a failure a test queued but never used, like
    // when it failed before downloading
    vi.mocked(extractPackInWorker).mockReset();
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

    describe('when the file cannot be loaded', () => {
      beforeEach(() => {
        // The failure is logged for debugging too
        vi.spyOn(console, 'error').mockImplementation(() => {});
      });

      async function loadFile() {
        const zip = await packZip({ 'us-ca-oakland.pmtiles/rocks.pmtiles': 'rocks' });
        const { container } = render(<Packs />);
        fireEvent.click(await screen.findByRole('tab', { name: 'Downloaded' }));
        const input = container.querySelector('input[type="file"]');
        if (!input) throw new Error('No file input');
        fireEvent.change(input, {
          target: { files: [new File([zip], 'us-ca-oakland.pmtiles.zip')] },
        });
      }

      it('says there is not enough storage space when the quota runs out', async () => {
        exceedQuota();
        await loadFile();
        expect((await screen.findByRole('alert')).textContent).toMatch(QUOTA_MESSAGE);
      });

      it('says the file could not be loaded when it fails for another reason', async () => {
        vi.mocked(extractPackInWorker).mockRejectedValueOnce(new Error('Not a zip'));
        await loadFile();
        expect((await screen.findByRole('alert')).textContent)
          .toMatch(/could not load us-ca-oakland.pmtiles.zip/i);
      });
    });
  });

  describe('when a pack download fails', () => {
    beforeEach(() => {
      // The failure is logged for debugging too
      vi.spyOn(console, 'error').mockImplementation(() => {});
    });

    async function startOaklandDownload() {
      render(<Packs />);
      await openCalifornia();
      await screen.findByText('Oakland');
      fireEvent.click(row('Oakland').getByRole('button', { name: 'download' }));
    }

    it('says there is not enough storage space when the quota runs out', async () => {
      exceedQuota();
      await startOaklandDownload();
      expect((await row('Oakland').findByRole('alert')).textContent).toMatch(QUOTA_MESSAGE);
      expect(row('Oakland').getByRole('button', { name: 'download' })).toBeTruthy();
    });

    it('does not fade the message with the row of a pack that is not downloaded', async () => {
      // MUI draws disabled buttons at low opacity, too faint to read red text on
      exceedQuota();
      await startOaklandDownload();
      const alert = await row('Oakland').findByRole('alert');
      expect(alert.closest('[aria-disabled="true"]')).toBeNull();
    });

    it('says the download failed when it fails for another reason', async () => {
      vi.mocked(extractPackInWorker).mockRejectedValueOnce(new Error('Network down'));
      await startOaklandDownload();
      expect((await row('Oakland').findByRole('alert')).textContent)
        .toMatch(/could not download/i);
    });

    it('says the download failed when the error mentions aborting but it was not stopped',
      async () => {
        // Like an IndexedDB transaction that aborted while storing the pack
        vi.mocked(extractPackInWorker).mockRejectedValueOnce(
          new Error('The transaction was aborted, so the request cannot be fulfilled.'),
        );
        await startOaklandDownload();
        expect((await row('Oakland').findByRole('alert')).textContent)
          .toMatch(/could not download/i);
      });

    it('says nothing failed when the download is stopped', async () => {
      const finishBerkeley = stallBerkeleyDownload(fetchMock, server);
      render(<Packs />);
      await openCalifornia();
      await screen.findByText('Berkeley');
      fireEvent.click(row('Berkeley').getByRole('button', { name: 'download' }));
      fireEvent.click(await row('Berkeley').findByRole('button', { name: 'stop' }));
      // The stalled read has to finish before the unzip notices it was stopped
      finishBerkeley();
      expect(await row('Berkeley').findByRole('button', { name: 'download' })).toBeTruthy();
      expect(row('Berkeley').queryByRole('alert')).toBeNull();
    });

    it('stops saying so when the pack is downloaded again', async () => {
      exceedQuota();
      await startOaklandDownload();
      await row('Oakland').findByRole('alert');
      fireEvent.click(row('Oakland').getByRole('button', { name: 'download' }));
      await waitFor(() => {
        expect(row('Oakland').getByRole('button', { name: 'delete' })).toBeTruthy();
      });
      expect(row('Oakland').queryByRole('alert')).toBeNull();
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

  describe('searching', () => {
    const searchBox = () => screen.getByRole('searchbox', { name: 'Search packs' });

    it('lists the All tab\'s matches flat, whatever level they are on', async () => {
      render(<Packs />);
      await screen.findByText('United States');

      fireEvent.change(searchBox(), { target: { value: 'berk' } });

      expect(await screen.findByText('Berkeley')).toBeTruthy();
      expect(screen.queryByText('Oakland')).toBeNull();
      expect(screen.queryByText('United States')).toBeNull();
    });

    it('says when nothing matches', async () => {
      render(<Packs />);
      await screen.findByText('United States');

      fireEvent.change(searchBox(), { target: { value: 'zzz' } });

      expect(screen.getByText('No packs match "zzz".')).toBeTruthy();
    });

    it('brings back the hierarchy when the search is cleared', async () => {
      render(<Packs />);
      await screen.findByText('United States');
      fireEvent.change(searchBox(), { target: { value: 'berk' } });
      await screen.findByText('Berkeley');

      fireEvent.click(screen.getByRole('button', { name: 'Clear search' }));

      expect(await screen.findByText('United States')).toBeTruthy();
      expect(screen.queryByText('Berkeley')).toBeNull();
    });

    it('filters the Downloaded tab', async () => {
      render(<Packs />);
      await openCalifornia();
      fireEvent.click(row('Berkeley').getByRole('button', { name: 'download' }));
      await waitFor(() => {
        expect(row('Berkeley').getByRole('button', { name: 'delete' })).toBeTruthy();
      });
      fireEvent.click(row('Oakland').getByRole('button', { name: 'download' }));
      await waitFor(() => {
        expect(row('Oakland').getByRole('button', { name: 'delete' })).toBeTruthy();
      });
      fireEvent.click(screen.getByRole('tab', { name: 'Downloaded' }));
      await screen.findByText('Berkeley');

      fireEvent.change(searchBox(), { target: { value: 'oak' } });

      expect(screen.getByText('Oakland')).toBeTruthy();
      expect(screen.queryByText('Berkeley')).toBeNull();
    });
  });

  describe('near me', () => {
    const nearMeButton = () => screen.getByRole('button', { name: 'Near me' });
    const OAKLAND = { latitude: 37.8, longitude: -122.27 };

    // jsdom has no geolocation, so each test says what the browser answers with
    function stubGeolocation(answer: (
      success: PositionCallback,
      error: PositionErrorCallback,
    ) => void) {
      const getCurrentPosition = vi.fn(answer);
      Object.defineProperty(navigator, 'geolocation', {
        configurable: true,
        value: { getCurrentPosition },
      });
      return getCurrentPosition;
    }

    const answerWith = ({ latitude, longitude }: typeof OAKLAND) => (
      success: PositionCallback,
    ) => success({
      coords: { latitude, longitude },
    } as GeolocationPosition);

    const listedNames = () => screen.getAllByRole('listitem').map(
      item => item.querySelector('.MuiListItemText-primary')?.textContent,
    );

    beforeEach(async () => {
      await publishPack(server, packMetadata({
        bbox: {
          bottom: 32.5, left: -124.5, right: -114.1, top: 42,
        },
        description: 'The whole state',
        id: 'us-ca',
        name: 'California (state)',
        pmtiles_path: 'us-ca.pmtiles.zip',
      }));
      await publishPack(server, packMetadata({
        bbox: {
          bottom: 38.8, left: -120.3, right: -119.9, top: 39.3,
        },
        description: 'The lake',
        id: 'us-ca-tahoe',
        name: 'Tahoe',
        pmtiles_path: 'us-ca-tahoe.pmtiles.zip',
      }));
    });

    afterEach(() => {
      Reflect.deleteProperty(navigator, 'geolocation');
    });

    it('lists the packs that cover the location flat, the smallest first', async () => {
      stubGeolocation(answerWith(OAKLAND));
      // Berkeley's box stops north of the location
      await publishPack(server, packMetadata({
        bbox: {
          bottom: 37.85, left: -122.3, right: -122.2, top: 37.9,
        },
        description: 'Just the town',
        id: 'us-ca-berkeley',
        name: 'Berkeley',
        pmtiles_path: BERKELEY_PATH,
      }));
      render(<Packs />);
      await screen.findByText('United States');

      fireEvent.click(nearMeButton());

      await screen.findByText('Oakland');
      expect(listedNames()).toEqual(['Oakland', 'California (state)']);
      expect(nearMeButton().getAttribute('aria-pressed')).toBe('true');
    });

    it('only lists packs that match the search too', async () => {
      stubGeolocation(answerWith(OAKLAND));
      render(<Packs />);
      await screen.findByText('United States');

      fireEvent.click(nearMeButton());
      fireEvent.change(
        screen.getByRole('searchbox', { name: 'Search packs' }),
        { target: { value: 'cali' } },
      );

      expect(await screen.findByText('California (state)')).toBeTruthy();
      expect(screen.queryByText('Oakland')).toBeNull();
      expect(screen.queryByText('Tahoe')).toBeNull();
    });

    it('says when no packs cover the location', async () => {
      stubGeolocation(answerWith({ latitude: 0, longitude: 0 }));
      render(<Packs />);
      await screen.findByText('United States');

      fireEvent.click(nearMeButton());

      expect(await screen.findByText('No packs cover your location.')).toBeTruthy();
    });

    it('brings back the hierarchy when turned off', async () => {
      stubGeolocation(answerWith(OAKLAND));
      render(<Packs />);
      await screen.findByText('United States');
      fireEvent.click(nearMeButton());
      await screen.findByText('Oakland');

      fireEvent.click(nearMeButton());

      expect(await screen.findByText('United States')).toBeTruthy();
      expect(screen.queryByText('Oakland')).toBeNull();
    });

    it('turns itself off and says why when the location is not available', async () => {
      stubGeolocation((_success, error) => error({
        code: 1,
        message: 'User denied Geolocation',
        PERMISSION_DENIED: 1,
        POSITION_UNAVAILABLE: 2,
        TIMEOUT: 3,
      } as GeolocationPositionError));
      render(<Packs />);
      await screen.findByText('United States');

      fireEvent.click(nearMeButton());

      expect(await screen.findByText(/Couldn't get your location/)).toBeTruthy();
      expect(nearMeButton().getAttribute('aria-pressed')).toBe('false');
      expect(screen.getByText('United States')).toBeTruthy();
    });

    it('asks for the location again each time it is turned on', async () => {
      const getCurrentPosition = stubGeolocation(answerWith(OAKLAND));
      render(<Packs />);
      await screen.findByText('United States');
      fireEvent.click(nearMeButton());
      await screen.findByText('Oakland');
      fireEvent.click(nearMeButton());
      await screen.findByText('United States');

      fireEvent.click(nearMeButton());

      await screen.findByText('Oakland');
      expect(getCurrentPosition).toHaveBeenCalledTimes(2);
    });
  });
});
