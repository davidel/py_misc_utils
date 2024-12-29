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


def create_module(name, code):
  path = get_mod_folder(create=True)
  mpath = os.path.join(path, f'{name}.py')

  with _LOCK:
    if os.path.exists(mpath):
      raise RuntimeError(f'Dynamic module "{name}" already exists: {mpath}')

    with open(mpath, mode='w') as f:
      f.write(code)

  return get_module(name)


def get_module(name):
  return importlib.import_module(f'{_MODNAME}.{name}')


def wrap_procfn_parent(kwargs):
  kwargs.update(dynamod_folder=get_mod_folder())

  return kwargs


def wrap_procfn_child(kwargs):
  path = kwargs.pop('dynamod_folder', None)
  if path is not None:
    set_mod_folder(path)

  return kwargs

