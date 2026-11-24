"""Tests for py_misc_utils.traceback (frame walking utilities).

Expectations pinned to the observed behavior (verified by probe scripts
before writing): get_frame(n) returns the frame n levels up from its
caller; get_frame_after(filename) walks up from the current frame
skipping frames within the given source file and returns the first
frame outside it (None if none); walk_stack() yields frames from the
starting frame up to the stack root (None start = caller's frame).
"""

from py_misc_utils import traceback as tb


def test_get_frame_zero_is_caller():
  """get_frame(0) returns the frame of the calling function."""

  def inner():
    return tb.get_frame(0)

  frame = inner()
  assert frame.f_code.co_name == 'inner'


def test_get_frame_levels():
  """get_frame(n) climbs n levels up from its own caller."""

  def level2():
    return tb.get_frame(1)

  def level1():
    frame = level2()
    return frame.f_code.co_name

  assert level1() == 'level1'


def test_get_frame_after_skips_current_file():
  """get_frame_after(__file__) returns the first frame outside this
  test module (i.e. the pytest runner frame)."""

  def leaf():
    return tb.get_frame_after(__file__)

  frame = leaf()
  assert frame is not None
  assert frame.f_code.co_filename != __file__


def test_get_frame_after_no_match_returns_none():
  """A filename absent from the stack yields None."""
  assert tb.get_frame_after('/no/such/file_xyz.py') is None


def test_walk_stack_from_frame():
  """walk_stack(frame) yields the frame first, then its callers."""

  def leaf():
    return list(tb.walk_stack())

  frames = leaf()
  names = [f.f_code.co_name for f in frames]

  assert names[0] == 'leaf'
  assert len(names) > 1


def test_walk_stack_with_start_frame():
  """An explicit start frame is the first yielded one."""

  def leaf():
    frame = tb.get_frame(0)

    return [f.f_code.co_name for f in tb.walk_stack(frame)]

  assert leaf()[0] == 'leaf'
