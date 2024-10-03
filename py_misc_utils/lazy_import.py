import importlib
import inspect


def lazy_import(name, modname=None, package=None):
  parent_frame = inspect.currentframe().f_back
  parent_globals = parent_frame.f_globals
  module = parent_globals.get(name)
  if module is None:
    if package == '.':
      package = getattr(inspect.getmodule(parent_frame), '__package__', None)

    module = importlib.import_module(modname or name, package=package)

    parent_globals[name] = module

  return module

