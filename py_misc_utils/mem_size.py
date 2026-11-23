"""Memory size estimation utilities for arbitrary Python objects.

This module provides functions to compute the memory footprint of Python
objects, including recursive traversal of containers such as dicts, lists,
and objects with __dict__ or __iter__ attributes. Custom type-specific size
functions can be registered for specialized types.
"""

import array
import sys
import threading

import numpy as np

from . import core_utils as cu

_LOCK = threading.Lock()
_TYPES_SIZE = dict()


def register(otype, sizefn):
  """Register a custom size function for a given object type.

  Args:
    otype: The object type to associate with the size function.
    sizefn: A callable that takes an object of type otype and returns its
      size in bytes.
  """
  with _LOCK:
    _TYPES_SIZE[otype] = sizefn


def _get_sizefn(otype):
  """Retrieve the registered size function for a given object type.

  Args:
    otype: The object type to look up.

  Returns:
    The registered size function, or None if none is registered.
  """
  with _LOCK:
    return _TYPES_SIZE.get(otype)


def std_sizefn(obj):
  """Return the size of an object using sys.getsizeof.

  Args:
    obj: Any Python object.

  Returns:
    The size of obj in bytes as reported by sys.getsizeof.
  """
  return sys.getsizeof(obj)


_SIZE_AWARE = {
    float,
    int,
    str,
    bytes,
    bytearray,
    array.array,
    np.ndarray,
}


def _get_size(obj, seen):
  """Recursively compute the memory size of an object.

  Args:
    obj: The object whose size is to be computed.
    seen: A set of object ids already visited, used to break cycles.

  Returns:
    The total memory size in bytes for obj and its referenced contents.
  """
  oid = id(obj)
  if oid in seen:
    return 0
  seen.add(oid)

  otype = type(obj)
  if sizefn := _get_sizefn(otype):
    size = sizefn(obj)
  else:
    size = sys.getsizeof(obj)
    if otype not in _SIZE_AWARE:
      if cu.isdict(obj):
        size += sum(_get_size(v, seen) + _get_size(k, seen) for k, v in obj.items())
      elif ustg := getattr(obj, 'untyped_storage', None):
        # Handle PyTorch tensors.
        size += sys.getsizeof(ustg())
      elif hasattr(obj, '__dict__'):
        size += _get_size(obj.__dict__, seen)
      elif hasattr(obj, '__iter__'):
        size += sum(_get_size(x, seen) for x in obj)

  return size


def get_size(obj):
  """Compute the total memory size of an object in bytes.

  Recursively traverses containers and references to estimate the full
  memory footprint of obj.

  Args:
    obj: Any Python object.

  Returns:
    The estimated total memory size in bytes.
  """
  return _get_size(obj, set())
