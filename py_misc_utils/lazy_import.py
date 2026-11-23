"""Lazily import a module, deferring actual loading until first use.

This module provides the :func:`lazy_import` function which imports a module
without eagerly executing its code. This is useful for optional dependencies
or for speeding up application startup by deferring expensive imports.
"""

import importlib.util
import inspect
import re
import sys

from . import traceback as tb


def lazy_import(modname, package=None):
  """Lazily import a module by name, optionally resolving relative package context.

  Args:
    modname: The name of the module to import. May be a dotted path.
    package: Optional package name used to resolve relative imports. If it
      consists only of dots (e.g. '..'), the caller's package context is used
      to resolve the absolute module name.

  Returns:
    The imported module object.
  """
  if package is not None and re.match(r'\.+$', package):
    parent_frame = tb.get_frame(1)
    parent_packages = inspect.getmodule(parent_frame).__package__.split('.')
    package_path = parent_packages[:len(parent_packages) - len(package) + 1]
    modname = '.'.join(package_path + [modname])
    package = None

  spec = importlib.util.find_spec(modname, package=package)
  loader = importlib.util.LazyLoader(spec.loader)
  spec.loader = loader
  module = importlib.util.module_from_spec(spec)
  sys.modules[modname] = module
  loader.exec_module(module)

  return module
