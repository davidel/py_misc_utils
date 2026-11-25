"""Dynamic module creation and management utilities.

This module provides functions for dynamically creating Python modules from
source code strings, storing them in a temporary directory, and importing them
at runtime. It uses a global namespace variable to manage the module folder
across process forks.
"""

import hashlib
import importlib
import os
import shutil
import sys
import threading

from . import global_namespace as gns

_MODROOT = 'pym'
# Root directory name under the temp root where dynamic modules are stored.


def _create_root():
  """Create and return the root directory for dynamic modules.

  Creates a directory named _MODROOT under the temp root if it doesn't exist.

  Returns:
    The filesystem path to the root module directory.
  """
  from . import tempdir as tmpd

  path = os.path.join(tmpd.get_temp_root(), _MODROOT)
  os.makedirs(path, exist_ok=True)

  return path


_MODNAME = '_dynamod'
# Name of the subdirectory under _MODROOT where dynamic module files live.


def _create_mod_folder():
  """Create the module folder and add its parent to sys.path.

  Creates _MODNAME under the root directory, writes an empty __init__.py,
  appends the root path to sys.path, and returns the module folder path.

  Returns:
    The filesystem path to the created module folder.
  """
  path = _create_root()
  dpath = os.path.join(path, _MODNAME)
  os.makedirs(dpath, exist_ok=True)

  with open(os.path.join(dpath, '__init__.py'), mode='w') as fd:
    pass

  sys.path.append(path)

  return dpath


def _clone_mod_folder(src_path):
  """Copy a source module folder into a new temp location for a child process.

  Copies the source folder tree into a new temporary _MODNAME directory,
  adjusts sys.path to point to the new location, and returns the new root path.
  This ensures each child process gets its own copy that is cleaned up when
  the process exits.

  Args:
    src_path: Filesystem path to the source module folder to copy.

  Returns:
    The filesystem path to the new root directory containing the copied module.
  """
  # This copies the source folder into a new temporary one for the new process,
  # which will be in turn deleted once this exists.
  # We cannot point directly to the source folder since it will be removed once
  # the parent exists.
  #
  # The source path may no longer exist if a previous test's temp directory was
  # already cleaned up (the gns.Var persists across tests but the temp dir does
  # not). In that case, fall back to creating a fresh module folder.
  if not os.path.exists(src_path):
    return _create_mod_folder()

  import tempfile as _tempfile

  # Use a unique destination root per call so src_path (which is itself a
  # previous call's destination root/_MODNAME) never collides with the copy
  # target.
  path = _tempfile.mkdtemp(prefix='pym_')
  shutil.copytree(src_path, os.path.join(path, _MODNAME))

  src_root = os.path.dirname(src_path)
  if src_root in sys.path:
    sys.path.remove(src_root)
  sys.path.append(path)

  return path


_LOCK = threading.RLock()
# Global reentrant lock protecting access to module creation and loading.

_MOD_FOLDER = gns.Var('dynamod.MOD_FOLDER',
                      child_fn=_clone_mod_folder,
                      defval=_create_mod_folder)
# Global namespace variable holding the dynamic module folder path,
# initialized on fork so child processes get their own copy.

_HASHNAME_LEN = int(os.getenv('DYNAMOD_HASHNAME_LEN', 12))
# Length of the hash suffix used in generated module names,
# from the DYNAMOD_HASHNAME_LEN environment variable (default 12).


def make_code_name(code):
  """Generate a deterministic module name from source code.

  Computes a SHA1 hash of the code and returns a name like '_hashed.<hash>'
  truncated to _HASHNAME_LEN characters.

  Args:
    code: The source code string to hash.

  Returns:
    A string module name derived from the code hash.
  """
  chash = hashlib.sha1(code.encode()).hexdigest()[:_HASHNAME_LEN]

  return f'_hashed._{chash}'


def create_module(name, code, overwrite=None):
  """Create a dynamic Python module from source code.

  Writes the given code to a .py file under the module folder using a dotted
  name as path components, then imports and returns the module. If the module
  already exists and overwrite is not allowed, raises RuntimeError. If overwrite
  is True and the file size hasn't changed, adds a comment hack to force reload
  (works around Python import subsystem issue #31772).

  Args:
    name: Dotted module name (e.g. 'foo.bar' becomes foo/bar.py).
    code: The source code string to write to the module file.
    overwrite: If True, overwrite an existing module. If None or False, raise
      RuntimeError if the module already exists. Defaults to None.

  Returns:
    The imported module object.

  Raises:
    RuntimeError: If the module already exists and overwrite is not True.
  """
  path = gns.get(_MOD_FOLDER)
  mpath = os.path.join(path, *name.split('.')) + '.py'

  with _LOCK:
    reload = False
    if os.path.exists(mpath):
      if overwrite in (None, False):
        raise RuntimeError(f'Dynamic module "{name}" already exists: {mpath}')
      else:
        # Note that there exist an issue with Python import subsystem:
        #
        #   https://bugs.python.org/issue31772
        #
        # Such issue causes a module to not be reloaded if the size of the source file
        # has not changed.
        # So BIG HACK here to add an headline comment if that's the case!
        reload = True
        if os.stat(mpath).st_size == len(code):
          code = f'# Note: Added due to https://bugs.python.org/issue31772\n\n{code}'

    os.makedirs(os.path.dirname(mpath), exist_ok=True)
    with open(mpath, mode='w') as f:
      f.write(code)

    module = get_module(name)

    return importlib.reload(module) if reload else module


def module_name(name):
  """Return the full importable name for a dynamic module.

  Prepends _MODNAME to the given name with a dot separator.

  Args:
    name: The module name (may contain dots).

  Returns:
    The full importable module name string.
  """
  return f'{_MODNAME}.{name}'


def get_module(name):
  """Import and return a dynamic module by name.

  Acquires the global lock and imports the module using module_name().

  Args:
    name: The module name to import.

  Returns:
    The imported module object.
  """
  with _LOCK:
    return importlib.import_module(module_name(name))
