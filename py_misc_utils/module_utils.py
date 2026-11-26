"""Dynamic module loading, importing, and introspection utilities.

This module provides functions for discovering, loading, installing, and
manipulating Python modules at runtime. It supports loading modules from
filesystem paths, resolving module hierarchies, importing module attributes
by dotted path, and cloning modules.
"""

import functools
import importlib
import importlib.util
import inspect
import os
import sys

from . import alog
from . import assert_checks as tas
from . import core_utils as cu
from . import fs_utils as fsu
from . import utils as ut

_PYINIT = '__init__.py'


def split_module_name(name):
  """Split a dotted module name into (parent, child) components.

  Args:
    name: The dotted module name string (e.g. 'a.b.c').

  Returns:
    A tuple (parent, child) where parent is everything before the last dot
    (or '' if there is no dot) and child is the part after the last dot
    (or the whole name if there is no dot).
  """
  pos = name.rfind('.')
  if pos < 0:
    return '', name

  return name[:pos], name[pos + 1:]


def add_sys_path(path):
  """Add a path to sys.path if it is not already present.

  Args:
    path: The filesystem path string to add to sys.path.
  """
  cu.append_if_missing(sys.path, path)


def find_module_parent(path):
  """Find the parent package __init__.py and remaining path parts for a module path.

  Walks up the directory tree from the given path looking for __init__.py files
    to determine the package parent and the relative module path within it.

  Args:
    path: The filesystem path to a module file.

  Returns:
    A tuple (init_path, remaining_parts) where init_path is the path to the
    nearest parent __init__.py and remaining_parts is a list of path components
    from that parent to the module name (without .py extension). Returns None
    if no parent package is found.
  """
  parts = fsu.path_split(path)
  modname = fsu.drop_ext(parts.pop(), '.py')

  for i in range(len(parts)):
    ipath = os.path.join(*parts[:i + 1], _PYINIT)
    if os.path.isfile(ipath):
      return ipath, parts[i + 1:] + [modname]


def find_module(path):
  """Find an already-imported module by its filesystem path.

  Args:
    path: The filesystem path to search for in sys.modules.

  Returns:
    A tuple (modname, module) if a module with the given absolute path is found
    in sys.modules, or None if no such module exists.
  """
  apath = os.path.abspath(path)

  for modname, module in sys.modules.items():
    mpath = module_file(module)
    if mpath is not None and mpath == apath:
      return modname, module


def install_module(modname, module):
  """Install a module into sys.modules under the given name.

  If a module with the same name already exists in sys.modules, verifies that
  both modules refer to the same file. Otherwise installs the new module.

  Args:
    modname: The name to register the module under in sys.modules.
    module: The module object to install.

  Raises:
      AssertionError: If a module with the same name already exists at a
        different file path.
  """
  xmodule = sys.modules.get(modname)
  if xmodule is not None:
    modfile, xmodfile = module_file(module), module_file(xmodule)
    tas.check_eq(modfile,
                 xmodfile,
                 msg=f'Module \"{modname}\" already defined at \"{xmodfile}\"')
  else:
    alog.debug(
        f'Installing module at \"{module_file(module)}\" with \"{modname}\" name')
    sys.modules[modname] = module


def load_module(path, modname=None, install=None, add_syspath=None):
  """Load a Python module from a filesystem path.

  If the module is already imported (found via find_module), returns the
  existing module. Otherwise loads it from disk using importlib, optionally
  installing it into sys.modules and adding its directory to sys.path.

  Args:
    path: Filesystem path to the .py module file.
    modname: Optional name to register the module under. If None, derived from
      the filename. Defaults to None.
    install: Whether to install the module into sys.modules. Defaults to True.
    add_syspath: Whether to add the module's directory to sys.path. Defaults to
      False.

  Returns:
    The loaded module object.
  """
  install = install or True
  add_syspath = add_syspath or False

  name_and_module = find_module(path)
  if name_and_module is not None:
    fmodname, module = name_and_module
    alog.debug(f'Found existing module \"{fmodname}\" for \"{path}\"')
    if install and modname is not None and modname != fmodname:
      install_module(modname, module)

    return module

  apath = os.path.abspath(path)

  parent = find_module_parent(apath) if os.path.basename(apath) != _PYINIT else None
  if parent is not None:
    init_path, mod_path = parent

    parent_module = load_module(init_path, install=True, add_syspath=add_syspath)

    for i in range(len(mod_path) - 1):
      partial = mod_path[:i + 1]
      ipath = os.path.join(os.path.dirname(parent_module.__file__), *partial, _PYINIT)
      if os.path.isfile(ipath):
        imodname = parent_module.__name__ + '.' + '.'.join(partial)
        load_module(ipath, modname=imodname, install=True)

    imodname = parent_module.__name__ + '.' + '.'.join(mod_path)
    alog.debug(f'Importing sub-module \"{imodname}\"')
    module = importlib.import_module(imodname)
  else:
    pathdir = syspath = os.path.dirname(apath)

    if modname is None:
      modname = fsu.drop_ext(os.path.basename(apath), '.py')
      if modname == '__init__':
        syspath, modname = os.path.split(pathdir)

    alog.debug(
        f'Installing module \"{apath}\" with name \"{modname}\" (syspath is \"{syspath}\")'
    )

    if add_syspath:
      add_sys_path(syspath)

    modspec = importlib.util.spec_from_file_location(
        modname, apath, submodule_search_locations=[pathdir])
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
  """Import a module by name or filesystem path.

  If name_or_path is a file path, loads it via load_module. Otherwise imports
  it as a top-level module using importlib.import_module, optionally installing
  it under an alternate name.

  Args:
    name_or_path: Module name string or filesystem path to a .py file.
    modname: Optional alternate name to install the module under in sys.modules.
      Defaults to None.
    install: Whether to install the module into sys.modules. Defaults to None
      (treated as True when modname is specified).
    add_syspath: Whether to add the module's directory to sys.path (only
      relevant for file paths). Defaults to None.
    package: Optional package name to resolve relative imports against.
      Defaults to None.

  Returns:
    The imported module object.
  """
  if os.path.isfile(name_or_path):
    module = load_module(name_or_path,
                         modname=modname,
                         install=install,
                         add_syspath=add_syspath)
  else:
    alog.debug(f'Loading module \"{name_or_path}')
    module = importlib.import_module(name_or_path, package=package)

    if modname is not None and install in (True, None):
      install_module(modname, module)

  return module


def _module_getter(dot_path, obj):
  """Resolve a dotted attribute path starting from a given object.

  Walks each component of dot_path, using getattr. If the current object is a
  module and an attribute is missing, attempts to import it as a submodule.

  Args:
    dot_path: Dot-separated attribute path string (e.g. 'a.b.c').
    obj: The starting object to resolve attributes from.

  Returns:
    The resolved object at the end of the dotted path.

  Raises:
    AttributeError: If an attribute cannot be resolved and is not importable
      as a submodule.
  """
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
  """Create a callable that resolves a dotted path on any object.

  Args:
    dot_path: Dot-separated attribute path string (e.g. 'a.b.c').

  Returns:
    A functools.partial callable that, when called with an object, resolves
    dot_path on that object using _module_getter.
  """
  return functools.partial(_module_getter, dot_path)


def import_module_names(modname, names=None):
  """Import specific names from a module and return them as a tuple.

  If names is None, derives a single name from the part of modname after the
  last dot and imports that attribute from the parent module.

  Args:
    modname: The module name to import from. May contain a dot to separate
      the module from the attribute name.
    names: Optional list or single name of attributes to import. If None, the
      name is derived from modname. Defaults to None.

  Returns:
    A tuple of the imported attribute objects from the module.
  """
  if names is None:
    npos = modname.find('.')
    tas.check_gt(npos, 0)
    names = [modname[npos + 1:]]
    modname = modname[:npos]
  else:
    names = ut.expand_strings(names)

  module = importlib.import_module(modname)

  return tuple(module_getter(name)(module) for name in names)


def rel_import_module(path, ppath, modname=None, install=None, add_syspath=None):
  """Import a module relative to another module's file path.

  Constructs an absolute path by joining the directory of ppath with
  '{path}.py' and imports it.

  Args:
    path: The module name (without .py) relative to ppath's directory.
    ppath: The filesystem path of the reference module.
    modname: Optional name to register the module under. Defaults to None.
    install: Whether to install into sys.modules. Defaults to None.
    add_syspath: Whether to add the module's directory to sys.path.
      Defaults to None.

  Returns:
    The imported module object.
  """
  ipath = os.path.abspath(os.path.join(os.path.dirname(ppath), f'{path}.py'))

  return import_module(ipath, modname=modname, install=install, add_syspath=add_syspath)


def maybe_import(modname, package=None):
  """Attempt to import a module, returning None on failure.

  Logs a spam-level message if the import fails rather than raising.

  Args:
    modname: The module name to import.
    package: Optional package to resolve relative imports against.
      Defaults to None.

  Returns:
    The imported module object, or None if the import fails.
  """
  try:
    return importlib.import_module(modname, package=package)
  except:
    alog.spam(f'Unable to load module \"{modname}\" in package \"{package}\"')


def module_file(module):
  """Return the __file__ attribute of a module, or None if not present.

  Args:
    module: A module object.

  Returns:
    The __file__ string attribute of the module, or None if the module has no
    __file__ attribute.
  """
  return getattr(module, '__file__', None)


def clone_module(modname):
  """Clone (re-import) a module by name, returning a fresh module object.

  Finds the module spec, creates a new module from it, and executes the
  loader to populate it. This produces a module independent of any cached
  version in sys.modules.

  Args:
    modname: The name of the module to clone.

  Returns:
    A newly loaded module object for modname.
  """
  specs = importlib.util.find_spec(modname)
  module = importlib.util.module_from_spec(specs)
  specs.loader.exec_module(module)

  return module
