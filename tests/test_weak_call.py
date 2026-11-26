"""Tests for py_misc_utils.weak_call.

WeakCall holds a weak reference to its target so that keeping the
WeakCall alive does not keep the target object alive: once the target
object is garbage collected, calls return the GONE sentinel.
"""

import gc

from py_misc_utils import weak_call


class Speaker:

  def __init__(self):
    self.hits = []

  def talk(self, a, b=0):
    self.hits.append((a, b))

    return a + b


def test_bound_method_call_merges_arguments():
  s = Speaker()
  wc = weak_call.WeakCall(s.talk, 1)

  assert wc(2) == 3
  assert s.hits == [(1, 2)]


def test_call_kwargs_override_default_kwargs():
  s = Speaker()
  wc = weak_call.WeakCall(s.talk)

  assert wc(1, b=3) == 4
  assert s.hits == [(1, 3)]


def test_call_kwargs_merge_over_construction_kwargs():

  def fn(a=0, b=0):
    return a, b

  wc = weak_call.WeakCall(fn, a=1, b=2)

  # Call-time kwargs override construction-time kwargs.
  assert wc(b=5) == (1, 5)


def test_target_collected_returns_gone():
  s = Speaker()
  wc = weak_call.WeakCall(s.talk)

  del s
  gc.collect()

  # Identity check: the sentinel is returned as-is, not a copy.
  assert wc() is weak_call.GONE


def test_plain_function_target():
  hits = []

  def fn(x):
    hits.append(x)

    return x + 1

  wc = weak_call.WeakCall(fn, 10)

  assert wc() == 11
  assert hits == [10]


def test_lambda_target():
  wc = weak_call.WeakCall(lambda x: x * 2, 21)

  assert wc() == 42
