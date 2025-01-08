import collections
import re
import string

from . import core_utils as cu


class ExpandHelper:

  def __init__(self, mappings):
    self.mappings = mappings

  def _parse_key(self, key):
    m = re.match(r'([^:]+):(.*)', key)

    return (m.group(1), m.group(2)) if m else (key, None)

  def __getitem__(self, key):
    lkey, defval = self._parse_key(key)

    value = cu.ns_lookup(lkey, self.mappings)
    if value is not None:
      return str(value)

    return self.substitute(defval) if defval is not None else defval

  def substitute(self, sdata):
    templ = string.Template(sdata)

    return templ.safe_substitute(self)


def var_expand(sdata, mappings, max_depth=10):
  # Prevent circular references by limiting lookups.
  helper = ExpandHelper(mappings)
  for n in range(max_depth):
    xdata = helper.substitute(sdata)
    if xdata == sdata:
      break
    sdata = xdata

  return xdata


def _expand_string(data, mappings):
  # The @KEY allows to reference any other data within the configuration.
  if (m := re.match(r'@([\w\.]+)$', data)) is not None:
    if (xdata := cu.ns_lookup(m.group(1), mappings)) is not None:
      return _expand(xdata, mappings)

  return var_expand(data, mappings)


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
    return _expand_string(data, mappings)
  elif hasattr(data, '__dict__'):
    for k, v in vars(data).items():
      setattr(data, k, _expand(v, mappings))

  return data


# This API allows configuration expansion of $VAR references, with mappings
# that are the input data iself, or, for example, os.environ.
# A string can reference ${A.B.C} which will be expanded to a value obtained
# from progressively looking up A (from the root mappings), then B within A,
# then C within B.
# Valid mappings are either dictionaries (mapping['A']) or namespaces/objects
# (mapping.A).
def expand(data, envs=None):
  mappings = [data]
  if envs:
    mappings.extend(envs)

  return _expand(data, mappings)

