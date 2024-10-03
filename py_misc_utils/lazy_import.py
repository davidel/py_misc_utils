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
        package = getattr(inspect.getmodule(parent_frame), '__package__', None)

      mstg.module = importlib.import_module(modname or name, package=package)

    return module

  return lazy

