import io
import pickle
import re

from . import alog
from . import module_utils as mu


class Unpickler(pickle.Unpickler):

  def __init__(self, *args,
               remaps=None,
               safe_globals=None,
               **kwargs):
    super().__init__(*args, **kwargs)
    self._remaps = remaps or dict()
    self._safe_globals = safe_globals

  def _check_safe_class(self, fqname):
    if self._safe_globals is not None:
      match = None
      for scrx in self._safe_globals:
        match = re.match(scrx, fqname)
        if match is not None:
          break

      if match is None:
        alog.xraise(RuntimeError, f'Unsafe global: {fqname}')

  def find_class(self, module, name):
    fqname = f'{module}.{name}'
    self._check_safe_class(fqname)

    remap = self._remaps.get(fqname, fqname)
    if remap != fqname:
      alog.debug(f'Unpickle remapping: {fqname} -> {remap}')

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

