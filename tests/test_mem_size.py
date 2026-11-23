"""Tests for py_misc_utils.mem_size.get_size.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): get_size() recursively accounts for containers
and their contents, never double counts shared objects, tolerates
reference cycles, honors registered per-type size functions (which
terminate recursion for that type), and the contents of strings and
bytes count as leaves (their payload is not traversed).
"""

import array
import sys

import numpy as np

from py_misc_utils import mem_size as mem


def test_primitive_sizes_are_positive():
  """Simple objects report a positive size."""
  assert mem.get_size(42) > 0
  assert mem.get_size('hello') > 0
  assert mem.get_size(3.14) > 0


def test_container_size_accounts_for_contents():
  """A list of ints sizes more than its own shell (getsizeof)."""
  empty = mem.get_size([])
  with_items = mem.get_size([1, 2, 3])
  assert with_items > empty


def test_dict_keys_and_values_both_counted():
  """Dict traversal accounts for keys and values."""
  small = mem.get_size({})
  big = mem.get_size({n: n for n in range(50)})
  assert big > small


def test_numpy_array_size_scales_with_data():
  """A larger ndarray reports a larger size."""
  small = mem.get_size(np.zeros(10))
  big = mem.get_size(np.zeros(10_000))
  assert big > small


def test_shared_objects_counted_once():
  """The same object referenced twice is accounted exactly once: the
  total difference is only the extra pointer slot of the container."""
  shared = [1, 2, 3]
  single = mem.get_size([shared])
  double = mem.get_size([shared, shared])
  shell_delta = sys.getsizeof([shared, shared]) - sys.getsizeof([shared])
  assert double - single == shell_delta
  assert double > single


def test_reference_cycles_terminate():
  """A self-referencing list does not recurse forever."""
  lst = [1, 2, 3]
  lst.append(lst)
  assert mem.get_size(lst) > 0


def test_registered_size_function_is_used():
  """register() associates a size function with a type."""

  class Custom:
    pass

  def my_size(obj):
    return 999

  mem.register(Custom, my_size)
  try:
    assert mem.get_size(Custom()) == 999
  finally:
    del mem._TYPES_SIZE[Custom]
  assert mem._get_sizefn(Custom) is None


def test_registered_size_function_terminates_recursion():
  """Registered types are not recursed into: the size function result
  is the whole size for objects of that type."""
  seen = []

  def note_size(obj):
    seen.append(1)
    return 8

  mem.register(list, note_size)
  try:
    assert mem.get_size([1, 2, 3]) == 8
  finally:
    del mem._TYPES_SIZE[list]
  assert seen == [1]


def test_std_sizefn_reports_getsizeof():
  """std_sizefn() is a thin wrapper over sys.getsizeof."""
  obj = [1, 2, 3]
  assert mem.std_sizefn(obj) == sys.getsizeof(obj)


def test_array_array_recursed_into():
  """array.array contents contribute to the total size."""
  small = mem.get_size(array.array('l', []))
  big = mem.get_size(array.array('l', range(100)))
  assert big > small
