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
// About 5px from that vertex at zoom 11, off to the side of the creek, and more than 10px from
// any other waterway
const NEAR_PALO_SECO_CREEK = { lat: 37.81161588879832, lng: -122.18207200824331 };
// A vertex of lower Sausal Creek, which Palo Seco Creek and several other creeks flow into
const SAUSAL_CREEK = { lat: 37.78774223089044, lng: -122.22367286682129 };
// The colors flowTrace.ts gives traces. Nothing else on the water map is magenta or green.
const DOWNSTREAM_RGB = [230, 0, 230];
const UPSTREAM_RGB = [0, 158, 58];
// The color mapStyles.ts gives natural waterways and waterbodies
const WATER_RGB = [31, 120, 180];

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

// How many pixels of the page are drawn in a color, leaving out the buttons that toggle traces,
// which use their trace's color for their icon while it's showing
async function pixelCount(page: Page, rgb: number[]) {
  const screenshot = PNG.sync.read(await page.screenshot({
    mask: [page.getByRole('button', { name: /^Trace (down|up)stream$/ })],
    maskColor: 'white',
  }));
  let count = 0;
  for (let offset = 0; offset < screenshot.data.length; offset += 4) {
    if (colorMatches([...screenshot.data.subarray(offset, offset + 3)], rgb)) count += 1;
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
  // The creeks and the bay, about 50,000 pixels once the map has drawn them
  let waterBefore = 0;
  await expect(async () => {
    waterBefore = await pixelCount(page, WATER_RGB);
    expect(waterBefore).toBeGreaterThan(10000);
  }).toPass({ timeout: 10_000 });

  const traceButton = page.getByRole('button', { name: 'Trace downstream' });
  await expect(traceButton).toHaveAttribute('aria-pressed', 'false');
  await traceButton.click();
  await expect(traceButton).toHaveAttribute('aria-pressed', 'true');
  // The trace runs from Palo Seco Creek down Sausal Creek, about 950 pixels at zoom 11. Far
  // fewer would mean it stopped short.
  await expect(async () => {
    expect(await pixelCount(page, DOWNSTREAM_RGB)).toBeGreaterThan(500);
  }).toPass({ timeout: 10_000 });
  // The rest of the water fades so the trace stands out, whatever colors someone can see
  expect(await pixelCount(page, WATER_RGB)).toBeLessThan(waterBefore / 10);

  await traceButton.click();
  await expect(traceButton).toHaveAttribute('aria-pressed', 'false');
  await expect(async () => {
    expect(await pixelCount(page, DOWNSTREAM_RGB)).toBe(0);
    expect(await pixelCount(page, WATER_RGB)).toBeGreaterThan(waterBefore * 0.9);
  }).toPass({ timeout: 10_000 });
  expect(alerts).toEqual([]);
});

test('traces where water comes from upstream of a waterway', async ({ page }) => {
  const alerts = collectAlerts(page);
  const { lat, lng } = SAUSAL_CREEK;
  await page.goto(`/#map=11/${lat}/${lng}&type=water`);
  await downloadOaklandPack(page);
  await expect(page.locator('.MapBottomSheetHeader h3')).toHaveText('Sausal Creek');

  const traceButton = page.getByRole('button', { name: 'Trace upstream' });
  await expect(traceButton).toHaveAttribute('aria-pressed', 'false');
  await traceButton.click();
  await expect(traceButton).toHaveAttribute('aria-pressed', 'true');
  // Sausal Creek and the creeks that flow into it, like Palo Seco and Shephard Creeks, about 650
  // pixels at zoom 11. Far fewer would mean it left some out.
  await expect(async () => {
    expect(await pixelCount(page, UPSTREAM_RGB)).toBeGreaterThan(350);
  }).toPass({ timeout: 10_000 });
  expect(await pixelCount(page, DOWNSTREAM_RGB)).toBe(0);

  await traceButton.click();
  await expect(traceButton).toHaveAttribute('aria-pressed', 'false');
  await expect(async () => {
    expect(await pixelCount(page, UPSTREAM_RGB)).toBe(0);
  }).toPass({ timeout: 10_000 });
  expect(alerts).toEqual([]);
});

test('picks a waterway that is a few pixels from the crosshairs', async ({ page }) => {
  const alerts = collectAlerts(page);
  const { lat, lng } = NEAR_PALO_SECO_CREEK;
  await page.goto(`/#map=11/${lat}/${lng}&type=water`);
  await downloadOaklandPack(page);
  // Waterways are only a couple of pixels wide, too thin to put the crosshairs right on
  await expect(page.locator('.MapBottomSheetHeader h3')).toHaveText('Palo Seco Creek');
  await expect(page.getByRole('button', { name: 'Trace downstream' })).toBeVisible();
  expect(alerts).toEqual([]);
});
