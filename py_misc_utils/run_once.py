import functools


def run_once(fn):

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    if not wrapper.has_run:
      wrapper.has_run = True

      return fn(*args, **kwargs)

  wrapper.has_run = False

  return wrapper

