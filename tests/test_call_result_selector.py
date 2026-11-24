"""Tests for py_misc_utils.call_result_selector.select.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): select(fn, idx) wraps a callable so the wrapper returns
element idx of the callable's result; functools.wraps preserves the
wrapped function's metadata; kwargs are forwarded.
"""

from py_misc_utils import call_result_selector as crs


def test_select_first():
  """select(fn, 0) returns the first element of fn's result."""
  fn = crs.select(lambda: (10, 20), 0)
  assert fn() == 10


def test_select_last():
  """select(fn, -1) supports negative indices like normal indexing."""
  fn = crs.select(lambda: [1, 2, 3], -1)
  assert fn() == 3


def test_select_args_kwargs_forwarded():
  """Positional and keyword arguments are forwarded to the callable."""

  def adder(a, b, c=0):
    return ('res', a + b + c)

  fn = crs.select(adder, 1)
  assert fn(1, 2) == 3
  assert fn(1, 2, c=10) == 13


def test_select_preserves_metadata():
  """functools.wraps copies __name__ and __doc__ onto the wrapper."""

  def documented():
    """Original doc."""
    return ('a', 'b')

  wrapped = crs.select(documented, 1)
  assert wrapped.__name__ == 'documented'
  assert wrapped.__doc__ == 'Original doc.'
