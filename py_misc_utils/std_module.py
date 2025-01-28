import functools
import importlib
import os
import sys

from . import core_utils as cu


def _module_origin(modname):
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
_STDLIB_PATHS = set(filter(lambda x: x is not None,
                           (_module_libpath(m) for m in _STDLIB_MODULES)))

@functools.cache
def is_std_module(modname):
  modname = cu.root_module(modname)
  lib_path = _module_libpath(modname)

  return lib_path is None or lib_path in _STDLIB_PATHS

