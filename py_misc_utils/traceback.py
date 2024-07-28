import os
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
  f = _get_frame().f_back
  while n > 0 and f is not None:
    f = f.f_back
    n -= 1

  return f


def walk_stack(f=None):
  if f is None:
    f = _get_frame().f_back

  while f is not None:
    yield f
    f = f.f_back

