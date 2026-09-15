const REPO = 'kueda/underfoot';

// Builds a URL that opens a new GitHub issue pre-filled with the current map
// view. The URL hash already encodes the pack, map type, and location (see
// urlHash.ts), so window.location.href alone is enough to reproduce it.
export function buildBugReportUrl(): string {
  const params = new URLSearchParams({
    labels: 'bug',
    body: [
      '## What went wrong?',
      '',
      '<!-- what were you doing, and what did you expect to happen instead? -->',
      '',
      '## Map link',
      '',
      window.location.href,
    ].join('\n'),
  });
  return `https://github.com/${REPO}/issues/new?${params.toString()}`;
}

// Opens a pre-filled GitHub issue so the reporter can describe the bug.
//
// Uses a real <a target="_blank"> click instead of window.open(): installed
// PWAs often render window.open() targets in an app-owned popup rather than
// handing them off to the system browser, but a genuine anchor click is more
// reliably treated as an out-of-scope navigation and routed out to it.
export function reportBug(): void {
  const link = document.createElement('a');
  link.href = buildBugReportUrl();
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
