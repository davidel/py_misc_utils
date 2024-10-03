import importlib
import inspect


class ModuleStorage:

  def __init__(self):
    self.module = None


def lazy_import(modname, package=None):
  if package.startswith('.'):
    parent_frame = inspect.currentframe().f_back
    parent_packages = inspect.getmodule(parent_frame).__package__.split('.')
    package_path = parent_packages[: len(parent_packages) - len(package) + 1]
    modname = '.'.join(package_path + [modname])
    package = None

  mstg = ModuleStorage()

  def lazy():
    module = mstg.module
    if module is None:
      mstg.module = module = importlib.import_module(modname, package=package)

    return module

  return lazy

