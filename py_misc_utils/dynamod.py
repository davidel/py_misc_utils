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


def module_name(name):
  return f'{_MODNAME}.{name}'


def create_module(name, code, overwrite=None):
  path = get_mod_folder(create=True)
  mod_parts = name.split('.')
  mpath = os.path.join(path, *mod_parts) + '.py'

  with _LOCK:
    if os.path.exists(mpath):
      if overwrite in (None, False):
        raise RuntimeError(f'Dynamic module "{name}" already exists: {mpath}')

    os.makedirs(os.path.dirname(mpath), exist_ok=True)
    with open(mpath, mode='w') as f:
      f.write(code)

    modname = module_name(name)
    module = sys.modules.get(modname)
    if module is not None:
      sys.modules[modname] = importlib.reload(module)

  return get_module(name)


def get_module(name):
  return importlib.import_module(module_name(name))


def wrap_procfn_parent(kwargs):
  kwargs.update(dynamod_folder=get_mod_folder())

  return kwargs


def wrap_procfn_child(kwargs):
  path = kwargs.pop('dynamod_folder', None)
  if path is not None:
    set_mod_folder(path)

  return kwargs

