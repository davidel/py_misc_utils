import io
import pickle

from . import alog
from . import module_utils as mu


class Unpickler(pickle.Unpickler):

  def __init__(self, *args,
               remaps=None,
               safe_modules=None,
               **kwargs):
    super().__init__(*args, **kwargs)
    self._remaps = remaps or dict()
    self._safe_modules = set(safe_modules) if safe_modules is not None else None

  def find_class(self, module, name):
    fqname = f'{module}.{name}'
    remap = self._remaps.get(fqname, fqname)
    if remap != fqname:
      alog.debug(f'Unpickle remapping: {fqname} -> {remap}')
    elif self._safe_modules is not None and module not in self._safe_modules:
      alog.xraise(RuntimeError, f'Unsafe module: {module}')

    return mu.import_module_names(remap)[0]


def load(*args, **kwargs):
  unpickler = Unpickler(*args, **kwargs)

  return unpickler.load()


def loads(data, *args, **kwargs):
  memfd = io.BytesIO(data)
  unpickler = Unpickler(memfd, *args, **kwargs)

  return unpickler.load()


# These are directly imported from the pickle module.
Pickler = pickle.Pickler
dump = pickle.dump
dumps = pickle.dumps

