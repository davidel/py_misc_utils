"""Select one element from the results returned by a callable.

This module provides the select decorator, which wraps a callable and
returns only the element at a given index from the callable's result.
"""

import functools


def select(fn, idx):
  """Wrap a callable to return a specific element of its result.

  Args:
    fn: Callable to wrap. Its result must support integer indexing.
    idx: Index of the element to return from the result.

  Returns:
    A wrapped callable that invokes fn and returns result[idx].
  """

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    """Run the wrapped callable and select one element of its result."""
    res = fn(*args, **kwargs)

    return res[idx]

  return wrapper
