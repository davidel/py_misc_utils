"""Tests for py_misc_utils.break_control.

Expectations pinned to the observed behavior: BreakControl is a context
manager registering a global SIGINT handler; hit() and frame() report
whether a break arrived and the frame; multiple instances stack (first
opens installs handler, last closes removes it); create() wraps open/close
with fin_wrap for automatic cleanup.
"""

import signal as sgn
import threading
import time

from py_misc_utils import break_control as bc


def test_open_close_no_break():
  """BreakControl opens/closes cleanly with no break received."""
  b = bc.BreakControl()
  b.open()
  assert b.hit() is False
  assert b.frame() is None
  b.close()


def test_break_control_context_manager():
  """BreakControl as context manager installs and removes handler."""
  with bc.BreakControl() as b:
    assert b.hit() is False
  assert b.hit() is False


def test_multiple_instances_stack():
  """Multiple BreakControl instances stack correctly."""
  b1 = bc.BreakControl()
  b2 = bc.BreakControl()
  b1.open()
  b2.open()
  assert b1.hit() is False
  assert b2.hit() is False
  b2.close()
  b1.close()


def test_simulate_break_records_frame():
  """Manually triggering SIGINT records the break and frame."""
  b = bc.BreakControl()
  b.open()

  def deliver_break():
    import os
    os.kill(os.getpid(), sgn.SIGINT)

  timer = threading.Timer(0.05, deliver_break)
  timer.start()
  time.sleep(0.2)

  assert b.hit() is True
  assert b.frame() is not None
  b.close()
  timer.cancel()
