# Using logging module directly here to avoid import the alog module, since this
# is supposed to be a lower level module with minimal/no local dependencies.
import logging
import traceback


def no_except(fn, *args, **kwargs):
  try:
    return fn(*args, **kwargs)
  except Exception as ex:
    # The logging.exception() API does emit in DEBUG level, and here we want something
    # a bit more higher level.
    tb = traceback.format_exc()
    logging.warning(f'Exception while running function: {ex}\n{tb}')

    return ex


def xwrap_fn(fn, *args, **kwargs):

  def fwrap():
    return no_except(fn, *args, **kwargs)

  return fwrap

