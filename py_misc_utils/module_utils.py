import functools
import importlib
import importlib.util
import inspect
import os
import sys

from . import alog
from . import assert_checks as tas
from . import fs_utils as fsu
from . import utils as ut


_PYINIT = '__init__.py'


def split_module_name(name):
  pos = name.rfind('.')
  if pos < 0:
    return '', name

  return name[: pos], name[pos + 1:]


def add_sys_path(path):
  ut.append_if_missing(sys.path, path)


def find_module_parent(path):
  parts = fsu.path_split(path)
  modname = fsu.drop_ext(parts.pop(), '.py')

  for i in range(len(parts)):
    ipath = os.path.join(*parts[: i + 1], _PYINIT)
    if os.path.isfile(ipath):
      return ipath, parts[i + 1:] + [modname]


def find_module(path):
  apath = os.path.abspath(path)

  for modname, module in sys.modules.items():
    mpath = module_file(module)
    if mpath is not None and mpath == apath:
      return modname, module


def install_module(modname, module):
  xmodule = sys.modules.get(modname)
  if xmodule is not None:
    modfile, xmodfile = module_file(module), module_file(xmodule)
    tas.check_eq(modfile, xmodfile,
                 msg=f'Module "{modname}" already defined at "{xmodfile}"')
  else:
    alog.debug(f'Installing module at "{module_file(module)}" with "{modname}" name')
    sys.modules[modname] = module


def load_module(path, modname=None, install=None, add_syspath=None):
  install = install or True
  add_syspath = add_syspath or False

  name_and_module = find_module(path)
  if name_and_module is not None:
    fmodname, module = name_and_module
    alog.debug(f'Found existing module "{fmodname}" for "{path}"')
    if install and modname is not None and modname != fmodname:
      install_module(modname, module)

    return module

  apath = os.path.abspath(path)

  parent = find_module_parent(apath) if os.path.basename(apath) != _PYINIT else None
  if parent is not None:
    init_path, mod_path = parent

    parent_module = load_module(init_path,
                                install=True,
                                add_syspath=add_syspath)

    for i in range(len(mod_path) - 1):
      partial = mod_path[: i + 1]
      ipath = os.path.join(os.path.dirname(parent_module.__file__), *partial, _PYINIT)
      if os.path.isfile(ipath):
        imodname = parent_module.__name__ + '.' + '.'.join(partial)
        load_module(ipath, modname=imodname, install=True)

    imodname = parent_module.__name__ + '.' + '.'.join(mod_path)
    alog.debug(f'Importing sub-module "{imodname}"')
    module = importlib.import_module(imodname)
  else:
    pathdir = syspath = os.path.dirname(apath)

    if modname is None:
      modname = fsu.drop_ext(os.path.basename(apath), '.py')
      if modname == '__init__':
        syspath, modname = os.path.split(pathdir)

    alog.debug(f'Installing module "{apath}" with name "{modname}" (syspath is "{syspath}")')

    if add_syspath:
      add_sys_path(syspath)

    modspec = importlib.util.spec_from_file_location(
      modname, apath,
      submodule_search_locations=[pathdir])
    module = importlib.util.module_from_spec(modspec)

    if install:
      install_module(modname, module)

    modspec.loader.exec_module(module)

  return module


def import_module(name_or_path,
                  modname=None,
                  install=None,
                  add_syspath=None,
                  package=None):
  if os.path.isfile(name_or_path):
    module = load_module(name_or_path,
                         modname=modname,
                         install=install,
                         add_syspath=add_syspath)
  else:
    alog.debug(f'Loading module "{name_or_path}')
    module = importlib.import_module(name_or_path, package=package)

    if modname is not None and install in (True, None):
      install_module(modname, module)

  return module


def _module_getter(dot_path, obj):
  for name in dot_path.split('.'):
    try:
      obj = getattr(obj, name)
    except AttributeError:
      if inspect.ismodule(obj):
        obj = importlib.import_module(obj.__name__ + '.' + name)
      else:
        raise

  return obj


def module_getter(dot_path):
  return functools.partial(_module_getter, dot_path)


def import_module_names(modname, names=None):
  if names is None:
    npos = modname.find('.')
    tas.check_gt(npos, 0)
    names = [modname[npos + 1:]]
    modname = modname[: npos]
  else:
    names = ut.expand_strings(name)

  module = importlib.import_module(modname)

  return tuple(module_getter(name)(module) for name in names)


def module_file(module):
  return getattr(module, '__file__', None)

