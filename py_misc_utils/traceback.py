"""Frame and stack walking utilities for tracing execution context.

This module provides utilities for inspecting and walking the Python call stack,
copied and adapted from the Python standard library's logging module.
"""

import sys

# Copied from logging module ...
if hasattr(sys, '_getframe'):

  def _get_frame():
    """Return the caller's frame using sys._getframe().

    Returns:
      The frame object one level up from this function.
    """
    return sys._getframe(1)

else:

  def _get_frame():
    """Return the caller's frame by raising and catching an exception.

    This is a fallback implementation used when sys._getframe() is not available.

    Returns:
      The frame object one level up from this function.
    """
    try:
      raise Exception
    except Exception as exc:  # noqa: BLE001 -- frame-capture idiom: Exception is raised as a carrier for its traceback, not caught as an error
      return exc.__traceback__.tb_frame.f_back


def get_frame(n=0):
  """Return the frame object n levels up from the current call stack.

  Args:
    n: Number of frames to go back from the current frame. Defaults to 0,
      meaning return the caller of get_frame().

  Returns:
    The frame object n levels up, or None if the stack is not deep enough.
  """
  frame = _get_frame().f_back
  while n > 0 and frame is not None:
    frame = frame.f_back
    n -= 1

  return frame


def get_frame_after(filename):
  """Find the first frame after the specified file in the call stack.

  Walks the stack starting from the current frame and returns the first frame
  whose code object is not in the given filename, after skipping frames that are
  inside that filename.

  Args:
    filename: The source filename to skip frames for.

  Returns:
    The first frame object after the given filename in the call stack, or None
    if no such frame exists.
  """
  frame, inscope = _get_frame(), False
  while frame is not None:
    if not inscope:
      if filename == frame.f_code.co_filename:
        inscope = True
    elif filename != frame.f_code.co_filename:
      break

    frame = frame.f_back

  return frame


def walk_stack(frame=None):
  """Yield frames walking up the call stack starting from the given frame.

  Args:
    frame: The starting frame object. If None, starts from the caller's frame
      (one level up). Defaults to None.

  Yields:
    Frame objects walking up the call stack, starting from the given frame and
    proceeding to each successive caller until None is reached.
  """
  if frame is None:
    frame = get_frame(1)

  while frame is not None:
    yield frame
    frame = frame.f_back
