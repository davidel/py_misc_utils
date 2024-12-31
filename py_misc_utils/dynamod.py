import hashlib
import importlib
import os
import shutil
import sys
import threading


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


_MOD_FOLDER = None
_LOCK = threading.RLock()

def _get_mod_folder(create=False):
  global _MOD_FOLDER

  with _LOCK:
    if _MOD_FOLDER is None and create:
      _MOD_FOLDER = _create_mod_folder()

    return _MOD_FOLDER


def _set_mod_folder(src_path):
  global _MOD_FOLDER

  path = _create_root()
  shutil.copytree(src_path, os.path.join(path, _MODNAME))

  src_root = os.path.dirname(src_path)
  if src_root in sys.path:
    sys.path.remove(src_root)
  sys.path.append(path)

  _MOD_FOLDER = path


_HASHNAME_LEN = int(os.getenv('DYNAMOD_HASHNAME_LEN', 8))

def make_code_name(code):
  chash = hashlib.sha1(code.encode()).hexdigest()[: _HASHNAME_LEN]

  return f'_hashed.ch_{chash}'


def create_module(name, code, overwrite=None):
  path = _get_mod_folder(create=True)
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


_FOLDER_KEY = 'dynamod_folder'

def wrap_procfn_parent(kwargs):
  kwargs.update({_FOLDER_KEY: _get_mod_folder()})

  return kwargs


def wrap_procfn_child(kwargs):
  path = kwargs.pop(_FOLDER_KEY, None)
  if path is not None:
    _set_mod_folder(path)

  return kwargs

