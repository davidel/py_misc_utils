import re
import string

from . import alog


class _FnDict:

  def __init__(self, lookup_fn):
    self._lookup_fn = lookup_fn

  def __getitem__(self, key):
    m = re.match(r'([^:]+):(.*)', key)
    if m:
      lkey, defval = m.group(1), m.group(2)
    else:
      lkey, defval = key, None

    return self._lookup_fn(lkey, defval=defval)


def _dict_lookup_fn(vals, delim, misses_ok):

  def lookup_fn(key, defval=None):
    value = vals.get(key, defval)
    if value is None:
      if not misses_ok:
        alog.xraise(KeyError, f'String template replace missing value for key: {key}')
      else:
        value = f'{delim}{key}'

    return value

  return lookup_fn


def template_replace(st, vals=None, lookup_fn=None, delim=None, misses_ok=None):
  delim = delim or '$'
  misses_ok = False if misses_ok is None else misses_ok

  class Template(string.Template):

    # Allow for brace ID with the format ${ID:DEFAULT_VALUE}.
    braceidpattern = r'((?a:[_a-z][_a-z0-9]*)(:[^}]*)?)'
    delimiter = delim

  if lookup_fn is None:
    lookup_fn = _dict_lookup_fn(vals, delim, misses_ok)

  return Template(st).substitute(_FnDict(lookup_fn))

