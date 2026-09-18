import path from 'node:path';
import { expect, test } from '@playwright/test';
import { PNG } from 'pngjs';

// A cut-down copy of the real pack, built by fixtures/make-oakland-pack.sh
const PACK_ZIP = path.join(import.meta.dirname, 'fixtures', 'us-ca-oakland.pmtiles.zip');

// Points inside large rock units where the map first opens the fixture pack, in the 1280x720
// Desktop Chrome viewport, and the fill colors mapStyles.ts gives their lithologies
const ROCK_UNIT_PIXELS = [
  { x: 250, y: 470, lithology: 'artificial', rgb: [255, 192, 203] },
  { x: 350, y: 400, lithology: 'sand', rgb: [255, 203, 35] },
  { x: 230, y: 560, lithology: 'water', rgb: [0, 15, 33] },
];
// Leeway per color channel, in case another platform's renderer blends slightly differently
const COLOR_TOLERANCE = 8;

const MANIFEST = {
  packs: [
    {
      admin1: 'United States',
      admin2: 'California',
      bbox: {
        bottom: 37.6249329829376,
        left: -122.37608299613,
        right: -122.00107120948,
        top: 37.9999225069647,
      },
      description: 'Oakland, CA, USA. Mostly for testing some place small.',
      id: 'us-ca-oakland',
      name: 'Oakland, CA, USA',
      pmtiles_path: 'us-ca-oakland.pmtiles.zip',
      updated_at: '2026-09-16T05:52:48.284014',
    },
  ],
  updated_at: '2026-09-16T05:52:48.284014',
};

// The app is on a different origin, so it can only read responses that allow it
const CORS_HEADERS = { 'Access-Control-Allow-Origin': '*' };

test.beforeEach(async ({ page }) => {
  // Stand in for static.underfoot.rocks so tests don't depend on the real CDN
  await page.route('https://static.underfoot.rocks/**', route => {
    const { pathname } = new URL(route.request().url());
    if (pathname === '/manifest.json') {
      return route.fulfill({ headers: CORS_HEADERS, json: MANIFEST });
    }
    if (pathname === '/us-ca-oakland.pmtiles.zip') {
      return route.fulfill({ headers: CORS_HEADERS, path: PACK_ZIP });
    }
    return route.fulfill({ headers: CORS_HEADERS, status: 404 });
  });
});

test('downloads a pack and shows its rocks on the map', async ({ page }) => {
  // The app shows an alert when a pack fails to load
  const alerts: string[] = [];
  page.on('dialog', dialog => {
    alerts.push(dialog.message());
    void dialog.dismiss();
  });

  await page.goto('/');
  await page.getByRole('button', { name: 'Download map data' }).click();
  const packsDialog = page.getByRole('dialog', { name: 'Packs' });
  const pack = packsDialog.getByRole('listitem').filter({ hasText: 'Oakland, CA, USA' });
  await pack.getByRole('button', { name: 'download' }).click();
  // Downloading a pack makes it the current pack when there isn't one
  await expect(pack.getByRole('radio')).toBeChecked();
  await packsDialog.getByRole('button', { name: 'close', exact: true }).click();
  await expect(packsDialog).toBeHidden();

  // The bottom sheet names the rock unit under the crosshairs, which only works once the map has
  // loaded the pack's tiles and read its rock unit attributes
  await expect(page.locator('.MapBottomSheetHeader h3')).toHaveText(
    'Joaquin Miller Formation (Late Cretaceous, Cenomanian)',
  );

  // The bottom sheet reads features from the loaded tiles, not from what WebGL drew, so also
  // check the drawn colors. MapLibre doesn't keep its drawing buffer around to read back, but a
  // screenshot captures the page as displayed.
  await expect(async () => {
    const screenshot = PNG.sync.read(await page.screenshot());
    for (const { x, y, lithology, rgb } of ROCK_UNIT_PIXELS) {
      const offset = (screenshot.width * y + x) * 4;
      const drawn = [...screenshot.data.subarray(offset, offset + 3)];
      const matches = drawn.every((value, i) => Math.abs(value - rgb[i]) <= COLOR_TOLERANCE);
      const where = `${lithology} at (${x}, ${y})`;
      expect(matches, `${where} is rgb(${drawn.join(',')}), not rgb(${rgb.join(',')})`).toBe(true);
    }
  }).toPass({ timeout: 10_000 });
  expect(alerts).toEqual([]);
});
