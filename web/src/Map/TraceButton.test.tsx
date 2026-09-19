import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import TraceButton from './TraceButton';

describe.each([
  ['downstream', 'Trace downstream'],
  ['upstream', 'Trace upstream'],
] as const)('TraceButton for tracing %s', (direction, label) => {
  function button() {
    return screen.queryByRole('button', { name: label });
  }

  it('is hidden when there is nothing to trace or clear', () => {
    render(
      <TraceButton direction={direction} traceable={false} active={false} onClick={() => {}} />,
    );
    expect(button()).toBeNull();
  });

  it('offers to trace a waterway', () => {
    render(<TraceButton direction={direction} traceable active={false} onClick={() => {}} />);
    expect(button()?.getAttribute('aria-pressed')).toBe('false');
  });

  it('stays visible while tracing so the trace can be cleared from anywhere', () => {
    render(<TraceButton direction={direction} traceable={false} active onClick={() => {}} />);
    expect(button()?.getAttribute('aria-pressed')).toBe('true');
  });

  it('calls onClick when tapped', () => {
    const onClick = vi.fn();
    render(<TraceButton direction={direction} traceable active={false} onClick={onClick} />);
    fireEvent.click(button()!);
    expect(onClick).toHaveBeenCalledOnce();
  });
});
