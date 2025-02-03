import hashlib
import importlib
import os
import shutil
import sys
import threading

from . import global_namespace as gns


_MODROOT = 'pym'

def _create_root():
  from . import tempdir as tmpd

  path = os.path.join(tmpd.get_temp_root(), _MODROOT)
  os.mkdir(path)

  return path


_MODNAME = '_dynamod'

def _create_mod_folder():
  path = _create_root()
  dpath = os.path.join(path, _MODNAME)
  os.mkdir(dpath)

  with open(os.path.join(dpath, '__init__.py'), mode='w') as fd:
    pass

  sys.path.append(path)

  return dpath


def _clone_mod_folder(src_path):
  # This copies the source folder into a new temporary one for the new process,
  # which will be in turn deleted once this exists.
  # We cannot point directly to the source folder since it will be removed once
  # the parent exists.
  path = _create_root()
  shutil.copytree(src_path, os.path.join(path, _MODNAME))

  src_root = os.path.dirname(src_path)
  if src_root in sys.path:
    sys.path.remove(src_root)
  sys.path.append(path)

  return path


_LOCK = threading.RLock()
_MOD_FOLDER = gns.Var('dynamod.MOD_FOLDER',
                      child_fn=_clone_mod_folder,
                      defval=_create_mod_folder)

_HASHNAME_LEN = int(os.getenv('DYNAMOD_HASHNAME_LEN', 12))

def make_code_name(code):
  chash = hashlib.sha1(code.encode()).hexdigest()[: _HASHNAME_LEN]

  return f'_hashed._{chash}'


def create_module(name, code, overwrite=None):
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
  return f'{_MODNAME}.{name}'


def get_module(name):
  with _LOCK:
    return importlib.import_module(module_name(name))

