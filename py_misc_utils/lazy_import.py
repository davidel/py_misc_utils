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
      if package == '.':
        parent_frame = inspect.currentframe().f_back
        lpackage = getattr(inspect.getmodule(parent_frame), '__package__', None)
      else:
        lpackage = package

      mstg.module = module = importlib.import_module(modname or name, package=lpackage)

    return module

  return lazy

