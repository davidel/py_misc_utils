import collections
import string


class ExpandHelper:

  def __init__(self, mappings):
    self.mappings = mappings

  def _try_lookup(self, ns, key):
    for part in key.split('.'):
      if isinstance(ns, collections.abc.Mapping):
        ns = ns.get(part)
      else:
        ns = getattr(ns, part, None)
      if ns is None:
        break

    return ns

  def __getitem__(self, key):
    for ns in self.mappings:
      value = self._try_lookup(ns, key)
      if value is not None:
        return str(value)


def var_expand(sdata, mappings):
  while True:
    templ = string.Template(sdata)
    xdata = templ.safe_substitute(ExpandHelper(mappings))
    if xdata == sdata:
      break
    sdata = xdata

  return xdata


def _expand(data, mappings):
  if isinstance(data, collections.abc.Mapping):
    for k, v in data.items():
      data[k] = _expand(v, mappings)
  elif isinstance(data, (list, tuple)):
    ndata = [_expand(v, mappings) for v in data]

    return type(data)(ndata)
  elif isinstance(data, set):
    return set(_expand(v, mappings) for v in data)
  elif isinstance(data, str):
    return var_expand(data, mappings)
  elif hasattr(data, '__dict__'):
    for k, v in vars(data).items():
      setattr(data, k, _expand(v, mappings))

  return data


def expand(data, envs=None):
  mappings = [data]
  if envs:
    mappings.extend(envs)

  return _expand(data, mappings)

