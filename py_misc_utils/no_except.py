import logging
import traceback


def no_except(fn, *args, **kwargs):
  try:
    return fn(*args, **kwargs)
  except Exception as ex:
    tb = traceback.format_exc()
    logging.warning(f'Exception while running function: {ex}\n{tb}')

    return ex


def xwrap_fn(fn, *args, **kwargs):

  def fwrap():
    return no_except(fn, *args, **kwargs)

  return fwrap

