import collections
import importlib
import os

from . import alog
from . import assert_checks as tas
from . import utils as ut


class DynLoader:

  def __init__(self, postfix, modname=None, path=None):
    if modname is not None:
      tas.check_is_none(path, msg=f'Cannot specify path="{path}" when specified modname="{modname}"')

      parent_mod = importlib.import_module(modname)
      mpath = os.path.dirname(parent_mod.__file__)
    else:
      tas.check_is_not_none(path, msg=f'Path must be specified if "modname" is missing')
      mpath = path

    module_names = []
    for fname, m in ut.re_enumerate_files(mpath, r'(.*)' + postfix + r'\.py$'):
      module_names.append(m.group(1))

    self._modules = collections.OrderedDict()
    for imod_name in sorted(module_names):
      imod = importlib.import_module(f'{modname}.{imod_name}{postfix}')
      mname = getattr(imod, 'MODULE_NAME', imod_name)
      self._modules[mname] = imod

  def module_names(self):
    return tuple(self._modules.keys())

  def modules(self):
    return tuple(self._modules.values())

  def get(self, name):
    return self._modules.get(name)

  def __getitem__(self, name):
    return self._modules[name]

  def __len__(self):
    return len(self._modules)

