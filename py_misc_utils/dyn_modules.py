import collections
import importlib
import os

from . import alog
from . import utils as ut


class DynLoader:

  def __init__(self, modname, postfix):
    parent_mod = importlib.import_module(modname)
    module_names = []
    for fname, m in ut.re_enumerate_files(os.path.dirname(parent_mod.__file__),
                                          r'(.*)' + postfix + r'\.py$'):
      module_names.append(m.group(1))

    self._modname = modname
    self._postfix = postfix
    self._modules = collections.OrderedDict()
    for imod_name in module_names:
      imod = importlib.import_module(f'{modname}.{imod_name}{postfix}')
      mname = getattr(imod, 'MODULE_NAME', imod_name)
      self._modules[mname] = imod

  def module_names(self):
    return tuple(self._modules.keys())

  def modules(self):
    return tuple(self._modules.values())

  def get(self, name, defval=None):
    return self._modules.get(name, defval)

  def __getitem__(self, name):
    return self._modules[name]

  def __len__(self):
    return len(self._modules)

