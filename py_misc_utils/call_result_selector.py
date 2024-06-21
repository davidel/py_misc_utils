import functools


def select(fn, idx):

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    res = fn(*args, **kwargs)

    return res[idx]

  return wrapper

