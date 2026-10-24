import { describe, expect, it } from 'vitest';

import { installPlatform } from './installPlatform';

const IPHONE_SAFARI = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) '
  + 'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1';
const IPHONE_CHROME = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) '
  + 'AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/138.0.7204.156 Mobile/15E148 Safari/604.1';
// iPadOS Safari claims to be a Mac by default
const IPAD_SAFARI = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
  + 'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15';
const MAC_SAFARI = IPAD_SAFARI;
const MAC_CHROME = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
  + 'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36';
const MAC_FIREFOX = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) '
  + 'Gecko/20100101 Firefox/141.0';
const WINDOWS_EDGE = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
  + '(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0';
const ANDROID_FIREFOX = 'Mozilla/5.0 (Android 15; Mobile; rv:141.0) Gecko/141.0 Firefox/141.0';
const ANDROID_CHROME = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 '
  + '(KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36';

describe('installPlatform', () => {
  it.each([
    ['iPhone Safari', IPHONE_SAFARI, 5, 'ios'],
    ['iPhone Chrome', IPHONE_CHROME, 5, 'ios'],
    ['iPad Safari', IPAD_SAFARI, 5, 'ios'],
    ['Mac Safari', MAC_SAFARI, 0, 'mac-safari'],
    ['Mac Chrome', MAC_CHROME, 0, 'desktop-chromium'],
    ['Windows Edge', WINDOWS_EDGE, 0, 'desktop-chromium'],
    ['Android Firefox', ANDROID_FIREFOX, 5, 'android'],
    ['Android Chrome', ANDROID_CHROME, 5, 'android'],
    ['Mac Firefox', MAC_FIREFOX, 0, 'other'],
  ])('recognizes %s', (_name, userAgent, maxTouchPoints, expected) => {
    expect(installPlatform(userAgent, maxTouchPoints)).toBe(expected);
  });
});
