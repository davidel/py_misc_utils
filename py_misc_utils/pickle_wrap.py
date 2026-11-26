"""Lazy (de)serialization wrapper for objects whose modules may not be loaded.

This module provides :func:`wrap` and :func:`unwrap` to convert non-serializable
module objects into :class:`PickleWrap` instances that defer actual loading
until the target module is available. Useful for delaying deserialization of
objects from optional dependencies.
"""

import pickle

from . import alog
from . import core_utils as cu
from . import inspect_utils as iu
from . import std_module as stdm

KNOWN_MODULES = {
    'builtins',
    'numpy',
    'pandas',
    'torch',
    cu.root_module(__name__),
}


def add_known_module(modname):
  """Register a module as known (i.e. no wrapping needed for its objects).

  Args:
    modname: The module name to add (its root module is registered).
  """
  KNOWN_MODULES.add(cu.root_module(modname))


def _needs_wrap(obj):
  """Check if an object needs to be wrapped.

  Args:
    obj: The object to check.

  Returns:
    True when the object module is neither known nor a standard module.
  """
  objmod = iu.moduleof(obj)
  if objmod is not None:
    modname = cu.root_module(objmod)
    if modname in KNOWN_MODULES or stdm.is_std_module(modname):
      return False

  return True


def _wrap(obj, pickle_module):
  """Recursively wrap the objects of a tree.

  Args:
    obj: The object tree to traverse.
    pickle_module: The pickle module used for serialization.

  Returns:
    The wrapped tree, or the original object when nothing was wrapped.
  """
  wrapped = 0
  if isinstance(obj, (list, tuple)):
    wobj = []
    for v in obj:
      wv = _wrap(v, pickle_module)
      if wv is not v:
        wrapped += 1

      wobj.append(wv)

    return type(obj)(wobj) if wrapped else obj
  elif cu.isdict(obj):
    wobj = type(obj)()
    for k, v in obj.items():
      wk = _wrap(k, pickle_module)
      if wk is not k:
        wrapped += 1
      wv = _wrap(v, pickle_module)
      if wv is not v:
        wrapped += 1

      wobj[wk] = wv

    return wobj if wrapped else obj
  elif isinstance(obj, PickleWrap):
    return obj
  elif _needs_wrap(obj):
    return PickleWrap(obj, pickle_module=pickle_module)
  else:
    return obj


def _unwrap(obj, pickle_module):
  """Recursively unwrap the PickleWrap instances of a tree.

  Args:
    obj: The object tree to traverse.
    pickle_module: The pickle module used for deserialization.

  Returns:
    The unwrapped tree; PickleWrap instances which cannot be loaded are
    kept as-is after logging a debug message.
  """
  unwrapped = 0
  if isinstance(obj, (list, tuple)):
    uwobj = []
    for v in obj:
      wv = _unwrap(v, pickle_module)
      if wv is not v:
        unwrapped += 1

      uwobj.append(wv)

    return type(obj)(uwobj) if unwrapped else obj
  elif cu.isdict(obj):
    uwobj = type(obj)()
    for k, v in obj.items():
      wk = _unwrap(k, pickle_module)
      if wk is not k:
        unwrapped += 1
      wv = _unwrap(v, pickle_module)
      if wv is not v:
        unwrapped += 1

      uwobj[wk] = wv

    return uwobj if unwrapped else obj
  elif isinstance(obj, PickleWrap):
    try:
      return obj.load(pickle_module=pickle_module)
    except Exception as ex:
      alog.debug(f'Unable to reload pickle-wrapped data ({obj.wrapped_class()}): {ex}')
      return obj
  else:
    return obj


def wrap(obj, pickle_module=pickle):
  """Recursively wrap objects whose modules are not yet known/loaded.

  Args:
    obj: The object to wrap (lists, dicts, and arbitrary objects are traversed).
    pickle_module: The pickle module to use for serialization.

  Returns:
    The wrapped object tree, with PickleWrap instances where needed.
  """
  return _wrap(obj, pickle_module)


def unwrap(obj, pickle_module=pickle):
  """Recursively unwrap PickleWrap instances by loading their wrapped data.

  Args:
    obj: The object tree containing PickleWrap instances.
    pickle_module: The pickle module to use for deserialization.

  Returns:
    The unwrapped object tree, with loaded objects replacing PickleWrap.
  """
  return _unwrap(obj, pickle_module)


class PickleWrap:
  """A lazily-loaded pickle container for objects from unloaded modules."""

  # Pickled qualified class name and data of the wrapped object.

  def __init__(self, obj, pickle_module=pickle):
    """Initialize the PickleWrap with an object and pickle module.

    Args:
      obj: The object to wrap.
      pickle_module: The pickle module used for serialization.
    """
    self._class = iu.qual_name(obj)
    self._data = pickle_module.dumps(obj)

  def wrapped_class(self):
    """Return the qualified class name of the wrapped object.

    Returns:
      A string like 'module.ClassName'.
    """
    return self._class

  def load(self, pickle_module=pickle):
    """Deserialize the wrapped object.

    Args:
      pickle_module: The pickle module used for deserialization.

    Returns:
      The deserialized object.
    """
    return pickle_module.loads(self._data)
