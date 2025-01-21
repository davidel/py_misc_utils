import io
import pickle
import re

from . import alog
from . import module_utils as mu


class Unpickler(pickle.Unpickler):

  def __init__(self, *args,
               remaps=None,
               safe_refs=None,
               **kwargs):
    super().__init__(*args, **kwargs)
    self._remaps = remaps or dict()
    self._safe_refs = safe_refs

  def find_class(self, module, name):
    fqname = f'{module}.{name}'
    remap = self._remaps.get(fqname, fqname)
    if remap != fqname:
      alog.debug(f'Unpickle remapping: {fqname} -> {remap}')
    elif self._safe_refs is not None:
      match = None
      for srrx in self._safe_refs:
        match = re.match(srrx, fqname)
        if match is not None:
          break

      if match is None:
        alog.xraise(RuntimeError, f'Unsafe reference: {fqname}')

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

