import hashlib
import importlib
import os
import shutil
import sys
import tempfile
import threading


_MODNAME = '_dynamod'

def _create_mod_folder():
  path = tempfile.mkdtemp()
  dpath = os.path.join(path, _MODNAME)
  os.makedirs(dpath)

  with open(os.path.join(dpath, '__init__.py'), mode='w') as fd:
    pass

  sys.path.append(path)

  from . import cleanups

  cleanups.register(shutil.rmtree, path, ignore_errors=True)

  return dpath


_MOD_FOLDER = None
_LOCK = threading.RLock()

def get_mod_folder(create=False):
  global _MOD_FOLDER

  with _LOCK:
    if _MOD_FOLDER is None and create:
      _MOD_FOLDER = _create_mod_folder()

    return _MOD_FOLDER


def set_mod_folder(path):
  global _MOD_FOLDER

  with _LOCK:
    _MOD_FOLDER = path
    if path not in sys.path:
      sys.path.append(path)


_HASHNAME_LEN = int(os.getenv('DYNAMOD_HASHNAME_LEN', 8))

def make_code_name(code):
  chash = hashlib.sha1(code.encode()).hexdigest()[: _HASHNAME_LEN]

  return f'_hashed.ch_{chash}'


def create_module(name, code, overwrite=None):
  path = get_mod_folder(create=True)
  mpath = os.path.join(path, *name.split('.')) + '.py'

  with _LOCK:
    # Note that there exist an issue with Python import subsystem:
    #
    #   https://bugs.python.org/issue31772
    #
    # Such issue causes a module to not be reloaded if the size of the source file
    # has not changed.
    if os.path.exists(mpath) and overwrite in (None, False):
      raise RuntimeError(f'Dynamic module "{name}" already exists: {mpath}')

    os.makedirs(os.path.dirname(mpath), exist_ok=True)
    with open(mpath, mode='w') as f:
      f.write(code)

    return importlib.reload(get_module(name))


def module_name(name):
  return f'{_MODNAME}.{name}'


def get_module(name):
  return importlib.import_module(module_name(name))


_FOLDER_KEY = 'dynamod_folder'

def wrap_procfn_parent(kwargs):
  kwargs.update({_FOLDER_KEY: get_mod_folder()})

  return kwargs


def wrap_procfn_child(kwargs):
  path = kwargs.pop(_FOLDER_KEY, None)
  if path is not None:
    set_mod_folder(path)

  return kwargs

