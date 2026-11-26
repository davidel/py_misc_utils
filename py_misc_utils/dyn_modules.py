"""Dynamic module loader for discovering and importing modules from directories.

This module provides the DynLoader class which can discover Python modules in a
directory or package, optionally matching a filename pattern, and import them
dynamically for use as plugin-like extensions.
"""

import collections
import importlib
import os

from . import assert_checks as tas
from . import gfs
from . import module_utils as mu


class DynLoader:
  """Dynamically loads Python modules from a directory or package path.

  Discovers .py files matching an optional postfix pattern in a given directory,
  imports them, and stores them in an ordered dictionary keyed by module name.
  Modules can optionally expose a MODULE_NAME attribute to override the derived
  name.
  """

  def __init__(self, modname=None, path=None, postfix=''):
    """Initialize the DynLoader and discover modules.

    Either modname or path must be specified (but not both). If modname is given,
    modules are discovered as submodules of that package. If path is given,
    modules are loaded directly from that filesystem path.

    Args:
      modname: Optional package/module name to discover submodules from. If
        specified, path must be None. Defaults to None.
      path: Optional filesystem path to a directory containing .py files to load.
        Required if modname is None. Defaults to None.
      postfix: Optional string suffix to match against module filenames (e.g.
        '_plugin' matches 'foo_plugin.py'). Defaults to ''.

    Raises:
      AssertionError: If both modname and path are specified, or if neither is
        specified.
    """
    if modname is not None:
      tas.check_is_none(
          path, msg=f'Cannot specify path="{path}" when specified modname="{modname}"')

      parent_mod = importlib.import_module(modname)
      mpath = os.path.dirname(parent_mod.__file__)
    else:
      tas.check_is_not_none(path, msg='Path must be specified if "modname" is missing')
      mpath = path

    module_names, matcher = [], gfs.RegexMatcher(r'(.*)' + postfix + r'\.py$')
    for fname in gfs.enumerate_files(mpath, matcher=matcher):
      module_names.append((matcher.match.group(1), os.path.join(mpath, fname)))

    self._modules = collections.OrderedDict()
    for (imod_name, imod_path) in sorted(module_names):
      if modname is not None:
        imod = importlib.import_module(f'{modname}.{imod_name}{postfix}')
      else:
        imod = mu.load_module(imod_path, modname=f'{imod_name}{postfix}')
      mname = getattr(imod, 'MODULE_NAME', imod_name)
      self._modules[mname] = imod

  def module_names(self):
    """Return the names of all loaded modules as a tuple.

    Returns:
      A tuple of strings, each being the name of a loaded module.
    """
    return tuple(self._modules.keys())

  def modules(self):
    """Return all loaded module objects as a tuple.

    Returns:
      A tuple of module objects in the order they were loaded.
    """
    return tuple(self._modules.values())

  def get(self, name):
    """Retrieve a loaded module by name.

    Args:
      name: The name of the module to retrieve.

    Returns:
      The loaded module object, or None if no module with that name exists.
    """
    return self._modules.get(name)

  def __getitem__(self, name):
    """Retrieve a loaded module by name using subscription syntax.

    Args:
      name: The name of the module to retrieve.

    Returns:
      The loaded module object.

    Raises:
      KeyError: If no module with that name exists.
    """
    return self._modules[name]

  def __len__(self):
    """Return the number of loaded modules.

    Returns:
      The count of modules currently loaded in this DynLoader instance.
    """
    return len(self._modules)
