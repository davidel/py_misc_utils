import importlib.util
import inspect
import sys


def lazy_import(modname, package=None):
  if package.startswith('.'):
    parent_frame = inspect.currentframe().f_back
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

