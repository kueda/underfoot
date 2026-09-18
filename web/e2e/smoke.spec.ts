import path from 'node:path';
import { expect, type Page, test } from '@playwright/test';
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

// A vertex of Palo Seco Creek in the fixture pack's water tiles, which flows into Sausal Creek
const PALO_SECO_CREEK = { lat: 37.810733294812074, lng: -122.18337535858156 };
// The color mapStyles.ts gives downstream traces. Nothing else on the water map is red.
const TRACE_RGB = [227, 26, 28];

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

// The app shows an alert when a pack fails to load
function collectAlerts(page: Page) {
  const alerts: string[] = [];
  page.on('dialog', dialog => {
    alerts.push(dialog.message());
    void dialog.dismiss();
  });
  return alerts;
}

async function downloadOaklandPack(page: Page) {
  await page.getByRole('button', { name: 'Download map data' }).click();
  const packsDialog = page.getByRole('dialog', { name: 'Packs' });
  const pack = packsDialog.getByRole('listitem').filter({ hasText: 'Oakland, CA, USA' });
  await pack.getByRole('button', { name: 'download' }).click();
  // Downloading a pack makes it the current pack when there isn't one
  await expect(pack.getByRole('radio')).toBeChecked();
  await packsDialog.getByRole('button', { name: 'close', exact: true }).click();
  await expect(packsDialog).toBeHidden();
}

function colorMatches(drawn: number[], rgb: number[]) {
  return drawn.every((value, i) => Math.abs(value - rgb[i]) <= COLOR_TOLERANCE);
}

// How many pixels of the page are drawn in the downstream trace's color, leaving out the button
// that toggles the trace, which uses that color for its icon while a trace is showing
async function tracePixelCount(page: Page) {
  const screenshot = PNG.sync.read(await page.screenshot({
    mask: [page.getByRole('button', { name: 'Trace downstream' })],
    maskColor: 'white',
  }));
  let count = 0;
  for (let offset = 0; offset < screenshot.data.length; offset += 4) {
    if (colorMatches([...screenshot.data.subarray(offset, offset + 3)], TRACE_RGB)) count += 1;
  }
  return count;
}

test('downloads a pack and shows its rocks on the map', async ({ page }) => {
  const alerts = collectAlerts(page);
  await page.goto('/');
  await downloadOaklandPack(page);

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
      const where = `${lithology} at (${x}, ${y})`;
      expect(
        colorMatches(drawn, rgb),
        `${where} is rgb(${drawn.join(',')}), not rgb(${rgb.join(',')})`,
      ).toBe(true);
    }
  }).toPass({ timeout: 10_000 });
  expect(alerts).toEqual([]);
});

test('traces where water flows downstream from a waterway', async ({ page }) => {
  const alerts = collectAlerts(page);
  // Opens the water map centered on the creek, which the map zooms to once the pack loads
  const { lat, lng } = PALO_SECO_CREEK;
  await page.goto(`/#map=11/${lat}/${lng}&type=water`);
  await downloadOaklandPack(page);
  await expect(page.locator('.MapBottomSheetHeader h3')).toHaveText('Palo Seco Creek');

  const traceButton = page.getByRole('button', { name: 'Trace downstream' });
  await expect(traceButton).toHaveAttribute('aria-pressed', 'false');
  await traceButton.click();
  await expect(traceButton).toHaveAttribute('aria-pressed', 'true');
  // The trace runs from Palo Seco Creek down Sausal Creek, about 950 pixels at zoom 11. Far
  // fewer would mean it stopped short.
  await expect(async () => {
    expect(await tracePixelCount(page)).toBeGreaterThan(500);
  }).toPass({ timeout: 10_000 });

  await traceButton.click();
  await expect(traceButton).toHaveAttribute('aria-pressed', 'false');
  await expect(async () => {
    expect(await tracePixelCount(page)).toBe(0);
  }).toPass({ timeout: 10_000 });
  expect(alerts).toEqual([]);
});
