import importlib
import inspect


class ModuleStorage:

  def __init__(self):
    self.module = None


def lazy_import(name, modname=None, package=None):
  mstg = ModuleStorage()

  def lazy():
    module = mstg.module
    if module is None:
      lmodname, lpackage = modname or name, package
      if lpackage.startswith('.'):
        parent_frame = inspect.currentframe().f_back
        parent_packages = inspect.getmodule(parent_frame).__package__.split('.')
        package_path = parent_packages[: len(parent_packages) - len(lpackage) + 1]
        lmodname = '.'.join(package_path + [lmodname])
        lpackage = None

      mstg.module = module = importlib.import_module(lmodname, package=lpackage)

    return module

  return lazy

