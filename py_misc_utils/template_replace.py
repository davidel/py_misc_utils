import re
import string

from . import assert_checks as tas


class _FnDict:

  def __init__(self, fn):
    self._fn = fn

  def __getitem__(self, key):
    m = re.match(r'([^:]+):(.*)', key)
    if m:
      lkey, defval = m.group(1), m.group(2)
    else:
      lkey, defval = key, None

    return self._fn(lkey, defval=defval)

  @staticmethod
  def dict_lookup_fn(d):
    def fn(k, defval=None):
      v = d.get(k, defval)
      tas.check_is_not_none(v, msg=f'String template replace missing value for key: {k}')

      return v

    return fn


def template_replace(st, vals=None, lookup_fn=None, delim=None):

  class _Template(string.Template):

    # Allow for brace ID with the format ${ID:DEFAULT_VALUE}.
    braceidpattern = r'((?a:[_a-z][_a-z0-9]*)(:[^}]*)?)'
    delimiter = delim or '$'

  if lookup_fn is None:
    lookup_fn = _FnDict.dict_lookup_fn(vals)

  return _Template(st).substitute(_FnDict(lookup_fn))

