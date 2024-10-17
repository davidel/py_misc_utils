import array
import sys
import threading

import numpy as np


_LOCK = threading.Lock()
_TYPES_SIZE = dict()

def register(otype, sizefn):
  with _LOCK:
    _TYPES_SIZE[otype] = sizefn


def _get_sizefn(otype):
  with _LOCK:
    return _TYPES_SIZE.get(otype)


def std_sizefn(obj):
  return sys.getsizeof(obj)


_SIZE_AWARE = {
  str,
  bytes,
  bytearray,
  array.array,
  np.ndarray,
}

def _get_size(obj, seen):
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
      if isinstance(obj, dict):
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
  return _get_size(obj, set())

