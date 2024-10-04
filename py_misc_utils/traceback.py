import sys


# Copied from logging module ...
if hasattr(sys, '_getframe'):

  def _get_frame():
    return sys._getframe(1)

else:

  def _get_frame():
    try:
      raise Exception
    except Exception as exc:
      return exc.__traceback__.tb_frame.f_back


def get_frame(n=0):
  frame = _get_frame().f_back
  while n > 0 and frame is not None:
    frame = frame.f_back
    n -= 1

  return frame


def walk_stack(frame=None):
  if frame is None:
    frame = get_frame(1)

  while frame is not None:
    yield frame
    frame = frame.f_back

