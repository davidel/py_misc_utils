import functools


def run_once(fn):

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    if not wrapper.has_run:
      result = fn(*args, **kwargs)
      wrapper.has_run = True
      return result

  wrapper.has_run = False

  return wrapper

