import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import DownstreamButton from './DownstreamButton';

function button() {
  return screen.queryByRole('button', { name: 'Trace downstream' });
}

describe('DownstreamButton', () => {
  it('is hidden when there is nothing to trace or clear', () => {
    render(<DownstreamButton traceable={false} active={false} onClick={() => {}} />);
    expect(button()).toBeNull();
  });

  it('offers to trace a waterway', () => {
    render(<DownstreamButton traceable active={false} onClick={() => {}} />);
    expect(button()?.getAttribute('aria-pressed')).toBe('false');
  });

  it('stays visible while tracing so the trace can be cleared from anywhere', () => {
    render(<DownstreamButton traceable={false} active onClick={() => {}} />);
    expect(button()?.getAttribute('aria-pressed')).toBe('true');
  });

  it('calls onClick when tapped', () => {
    const onClick = vi.fn();
    render(<DownstreamButton traceable active={false} onClick={onClick} />);
    fireEvent.click(button()!);
    expect(onClick).toHaveBeenCalledOnce();
  });
});
