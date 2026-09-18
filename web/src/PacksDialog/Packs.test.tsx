import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import localforage from 'localforage';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  packMetadata, publishPack, StaticServer, stubStaticServer,
} from '../test/packFixtures';
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

  describe('when a pack download finishes while another is still downloading', () => {
    it('keeps showing the other download in progress', async () => {
      const finishBerkeley = stallBerkeleyDownload(fetchMock, server);
      render(<Packs />);
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
});
