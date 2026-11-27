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
  """A pickle.Unpickler that remaps class names and filters unsafe references.

  Example:
    import io
    import pickle
    import py_misc_utils.obj as obj
    data = pickle.dumps(obj.Obj())
    unpickler = remap_pickle.Unpickler(io.BytesIO(data))
    # Remap a class reference to the module it moved to:
    unpickler.find_class('py_misc_utils.obj', 'Obj')
    # -> <class 'py_misc_utils.obj.Obj'>
  """

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

    A remapped reference bypasses the safe_refs check, since it was
    explicitly mapped to its new name.

    Args:
      module: The module name from the pickle stream.
      name: The class name from the pickle stream.

    Returns:
      The resolved class object.

    Raises:
      RuntimeError: If safe_refs is set and the (non-remapped) reference
        is not in the whitelist.
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
    *args: The file to load from, plus any Unpickler positional argument.
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

  The returned module can be handed over to code (or libraries) expecting
  the pickle module, so that all its (de)serializations go through the
  remapping/safety policy.

  Args:
    **kwargs: Default keyword arguments for load/loads (remaps, safe_refs).

  Returns:
    A module clone exposing load and loads with remapping support.

  Example:
    import pickle
    import pandas
    pmod = remap_pickle.make_module(safe_refs={'builtins.dict'})
    pmod.loads(pickle.dumps(dict(a=1)))  # OK - dict is whitelisted.
    pmod.loads(pickle.dumps(pandas.DataFrame()))
    # RuntimeError: pandas.DataFrame is not whitelisted.
  """
  module = mu.clone_module('pickle')

  module.load = functools.partial(load, **kwargs)
  module.loads = functools.partial(loads, **kwargs)

  return module
