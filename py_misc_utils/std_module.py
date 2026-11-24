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
    return os.path.dirname(origin)


def _stdlib_root(lib_path, stdlib_paths):
  """Resolve a module directory to its stdlib root directory - if any.

  Args:
    lib_path: The directory containing a module, as returned by
      _module_libpath().
    stdlib_paths: The set of known stdlib root directories.

  Returns:
    The ancestor directory of lib_path which is a member of stdlib_paths,
    or None if lib_path is not below any of them, or if lib_path is located
    inside a package installation directory (site-packages / dist-packages).
  """
  while lib_path:
    if os.path.basename(lib_path) in _NON_STDLIB_DIR_NAMES:
      return None

    if lib_path in stdlib_paths:
      return lib_path

    parent = os.path.dirname(lib_path)
    if parent == lib_path:
      break

    lib_path = parent

  return None


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

# Package installation directories. They can be nested inside the standard
# library directory (e.g. <prefix>/lib/pythonX.Y/site-packages), so any
# module found below them must be classified as third-party even when one
# of their ancestors is a stdlib directory.
_NON_STDLIB_DIR_NAMES = frozenset(('site-packages', 'dist-packages'))


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
  if lib_path is None:
    return True

  return _stdlib_root(lib_path, _STDLIB_PATHS) is not None
