"""Pickle deserialization with module/class remapping and safe reference filtering.

This module provides a custom :class:`Unpickler` that remaps fully-qualified
class names during unpickling and optionally restricts loading to a whitelist
of safe references. Helper functions :func:`load`, :func:`loads`, and
:func:`make_module` make integration easy.
"""

import functools
import io
import pickle

from . import alog
from . import module_utils as mu


class Unpickler(pickle.Unpickler):
  """A pickle.Unpickler that remaps class names and filters unsafe references."""

  def __init__(self, *args, remaps=None, safe_refs=None, **kwargs):
    """Initialize the Unpickler with remapping and safety configuration.

    Args:
      *args: Positional arguments passed to pickle.Unpickler.
      remaps: Optional mapping of fully-qualified names to remapped names.
      safe_refs: Optional set of allowed fully-qualified names. If provided,
        any reference not in the set raises RuntimeError.
      **kwargs: Additional keyword arguments passed to pickle.Unpickler.
    """
    super().__init__(*args, **kwargs)
    self._remaps = remaps or dict()
    self._safe_refs = set(safe_refs) if safe_refs is not None else None

  def find_class(self, module, name):
    """Resolve a class reference, applying remaps and safety checks.

    Args:
      module: The module name from the pickle stream.
      name: The class name from the pickle stream.

    Returns:
      The resolved class object.
    """
    fqname = f'{module}.{name}'
    remap = self._remaps.get(fqname, fqname)
    if remap != fqname:
      alog.debug(f'Unpickle remapping: {fqname} -> {remap}')
    elif self._safe_refs is not None and fqname not in self._safe_refs:
      alog.xraise(RuntimeError, f'Unsafe reference: {fqname}')

    return mu.import_module_names(remap)[0]


def load(*args, **kwargs):
  """Load an object from a file using the remapping Unpickler.

  Args:
    *args: Positional arguments passed to Unpickler.
    **kwargs: Keyword arguments passed to Unpickler (remaps, safe_refs).

  Returns:
    The unpickled object.
  """
  unpickler = Unpickler(*args, **kwargs)

  return unpickler.load()


def loads(data, *args, **kwargs):
  """Load an object from bytes using the remapping Unpickler.

  Args:
    data: The pickled bytes to deserialize.
    *args: Positional arguments passed to Unpickler.
    **kwargs: Keyword arguments passed to Unpickler (remaps, safe_refs).

  Returns:
    The unpickled object.
  """
  memfd = io.BytesIO(data)
  unpickler = Unpickler(memfd, *args, **kwargs)

  return unpickler.load()


def make_module(**kwargs):
  """Create a pickle-like module clone with remapping load functions.

  Args:
    **kwargs: Default keyword arguments for load/loads (remaps, safe_refs).

  Returns:
    A module clone exposing load and loads with remapping support.
  """
  module = mu.clone_module('pickle')

  module.load = functools.partial(load, **kwargs)
  module.loads = functools.partial(loads, **kwargs)

  return module
