"""Standard library module detection utilities.

This module provides :func:`is_std_module` to determine whether a module
originates from the Python standard library, based on comparing its library
path against paths derived from a set of known stdlib modules.
"""

import functools
import importlib
import os
import sys

from . import core_utils as cu


def _module_origin(modname):
  """Return the filesystem origin path of a module, importing it if needed.

  Args:
    modname: The module name to inspect.

  Returns:
    The module's __file__ or __spec__.origin path, or None if the module is
    not found or has no origin (e.g. built-in modules).
  """
  module = sys.modules.get(modname)
  if module is None:
    try:
      module = importlib.import_module(modname)
    except ModuleNotFoundError:
      pass

  if module is not None:
    path = getattr(module, '__file__', None)
    if path is None:
      spec = getattr(module, '__spec__', None)
      path = spec.origin if spec is not None else None

    return path


def _module_libpath(modname):
  """Return the library directory of a module.

  Args:
    modname: The module name to inspect.

  Returns:
    The directory containing the module, or None if the module has no origin
    or is built-in.
  """
  origin = _module_origin(modname)
  if origin not in {None, 'built-in'}:
    lib_path = os.path.dirname(origin)

    return lib_path if lib_path else None


# Some of the standard modules. Should be enough to get coverage of the
# Python standard library path (there are more than one since some might
# turn "built-in" and not have a __file__ or __spec__).
_STDLIB_MODULES = (
    'abc',
    'copy',
    'io',
    'os',
    'pickle',
    'random',
    'string',
    'types',
)
_STDLIB_PATHS = set(
    filter(lambda x: x is not None, (_module_libpath(m) for m in _STDLIB_MODULES)))


@functools.cache
def is_std_module(modname):
  """Check whether a module belongs to the Python standard library.

  Modules without an origin (built-in) are considered stdlib.

  Args:
    modname: The module name to check (dotted paths use the root module).

  Returns:
    True if the module is a stdlib module, False otherwise.
  """
  modname = cu.root_module(modname)
  lib_path = _module_libpath(modname)

  return lib_path is None or lib_path in _STDLIB_PATHS
