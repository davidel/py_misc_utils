import functools
import importlib
import importlib.util
import io
import pickle

from . import alog
from . import module_utils as mu


class Unpickler(pickle.Unpickler):

  def __init__(self, *args,
               remaps=None,
               safe_refs=None,
               **kwargs):
    super().__init__(*args, **kwargs)
    self._remaps = remaps or dict()
    self._safe_refs = set(safe_refs) if safe_refs is not None else None

  def find_class(self, module, name):
    fqname = f'{module}.{name}'
    remap = self._remaps.get(fqname, fqname)
    if remap != fqname:
      alog.debug(f'Unpickle remapping: {fqname} -> {remap}')
    elif self._safe_refs is not None and fqname not in self._safe_refs:
      alog.xraise(RuntimeError, f'Unsafe reference: {fqname}')

    return mu.import_module_names(remap)[0]


def load(*args, **kwargs):
  unpickler = Unpickler(*args, **kwargs)

  return unpickler.load()


def loads(data, *args, **kwargs):
  memfd = io.BytesIO(data)
  unpickler = Unpickler(memfd, *args, **kwargs)

  return unpickler.load()


def make_module(**kwargs):
  specs = importlib.util.find_spec('pickle')
  module = importlib.util.module_from_spec(specs)
  specs.loader.exec_module(module)

  module.load = functools.partial(load, **kwargs)
  module.loads = functools.partial(loads, **kwargs)

  return module

