/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { VitePWA } from 'vite-plugin-pwa';
import react from '@vitejs/plugin-react';
import basicSsl from '@vitejs/plugin-basic-ssl';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react(),

    // Allows self-signed certificate SSL support for local development
    basicSsl(),

    VitePWA({
      // 'autoUpdate' reloads the page as soon as a new service worker activates,
      // which wipes out whatever the user is doing. 'prompt' hands the decision
      // to onNeedRefresh in main.tsx.
      registerType: 'prompt',
      devOptions: {
        enabled: true,
      },
      includeAssets: [
        'favicon.ico',
        'icon-180x180.png',
        'icon-192x192.png',
        'icon-512x512.png',
        'mask-icon.svg',
        'fonts/**/*.pbf',
      ],
      manifest: {
        name: 'Underfoot',
        short_name: 'Underfoot',
        description: 'The world beneath your feet',
        theme_color: '#000000',
        background_color: '#ffffff',
        icons: [
          {
            src: 'icon-192x192.png',
            sizes: '192x192',
            type: 'image/png',
          },
          {
            src: 'icon-512x512.png',
            sizes: '512x512',
            type: 'image/png',
          },
          {
            src: 'icon-512x512.png',
            sizes: '512x512',
            type: 'image/png',
            purpose: 'any',
          },
          {
            src: 'icon-512x512.png',
            sizes: '512x512',
            type: 'image/png',
            purpose: 'maskable',
          },
        ],
      },
    }),
  ],
  test: {
    environment: 'jsdom',
    // Playwright runs the e2e/ tests
    include: ['src/**/*.test.{ts,tsx}'],
    restoreMocks: true,
    setupFiles: ['./src/test/setup.ts'],
    unstubGlobals: true,
  },
});
