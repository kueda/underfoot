import { defineConfig, devices } from '@playwright/test';

const PORT = 4174;

export default defineConfig({
  testDir: './e2e',
  forbidOnly: !!process.env.CI,
  use: {
    baseURL: `https://localhost:${PORT}`,
    // vite preview uses the self-signed certificate from @vitejs/plugin-basic-ssl
    ignoreHTTPSErrors: true,
    // A service worker can answer requests before page.route sees them
    serviceWorkers: 'block',
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    // Build every run so tests never see a stale dist/. This isn't vite preview's default port,
    // so a preview server started by hand won't get in the way.
    command: `npx vite build && npx vite preview --port ${PORT} --strictPort`,
    ignoreHTTPSErrors: true,
    reuseExistingServer: false,
    timeout: 120_000,
    url: `https://localhost:${PORT}`,
  },
});
