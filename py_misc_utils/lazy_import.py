import importlib.util
import inspect
import re
import sys

from . import traceback as tb


def lazy_import(modname, package=None):
  if package is not None and re.match(r'\.+$', package):
    parent_frame = tb.get_frame(1)
    parent_packages = inspect.getmodule(parent_frame).__package__.split('.')
    package_path = parent_packages[: len(parent_packages) - len(package) + 1]
    modname = '.'.join(package_path + [modname])
    package = None

  spec = importlib.util.find_spec(modname, package=package)
  loader = importlib.util.LazyLoader(spec.loader)
  spec.loader = loader
  module = importlib.util.module_from_spec(spec)
  sys.modules[modname] = module
  loader.exec_module(module)

  return module

