"""String template substitution with custom delimiters and lookup functions.

This module provides :func:`template_replace` which substitutes placeholders
in a string using a customizable delimiter, supporting default values
(${ID:DEFAULT}) and pluggable lookup functions for dictionary, attribute,
or other value resolution strategies.
"""

import re
import string

from . import alog


class _FnDict:
  """A mapping-like object that resolves keys via a lookup function.

  Keys may include a ':default' suffix which is passed to the lookup
  function as the defval argument.
  """

  def __init__(self, lookup_fn):
    """Initialize the _FnDict with a lookup function.

    Args:
      lookup_fn: A callable taking (key, defval) and returning the
        substituted value.
    """
    self._lookup_fn = lookup_fn

  def __getitem__(self, key):
    """Resolve a key through the lookup function.

    Args:
      key: The placeholder key, optionally with a ':default' suffix.

    Returns:
      The substituted value.
    """
    m = re.match(r'([^:]+):(.*)', key)
    if m:
      lkey, defval = m.group(1), m.group(2)
    else:
      lkey, defval = key, None

    return self._lookup_fn(lkey, defval=defval)


def _dict_lookup_fn(vals, delim, misses_ok):
  """Create a lookup function backed by a dictionary.

  Args:
    vals: The dictionary of key/value pairs.
    delim: The template delimiter used for missing value markers.
    misses_ok: If True, missing keys resolve to '<delim><key>' instead of
      raising KeyError.

  Returns:
    A lookup function taking (key, defval).
  """

  def lookup_fn(key, defval=None):
    """Resolve a key from the dictionary, with optional default handling.

    Args:
      key: The lookup key.
      defval: Optional default value.

    Returns:
      The value, defval, or a missing marker, or raises KeyError.
    """
    value = vals.get(key, defval)
    if value is None:
      if not misses_ok:
        alog.xraise(KeyError, f'String template replace missing value for key: {key}')
      else:
        value = f'{delim}{key}'

    return value

  return lookup_fn


def defval_lookup(vals, default):
  """Create a lookup function falling back to a default value.

  Args:
    vals: The dictionary of key/value pairs.
    default: Default value used when a key is missing; when None the
      missing placeholders resolve to '<delim><key>'.

  Returns:
    A lookup function taking (key, defval).
  """

  def lookup_fn(key, defval=None):
    """Resolve a key from the dictionary, falling back to the default.

    Args:
      key: The lookup key.
      defval: Optional default value returned when the key is missing.

    Returns:
      The value, the default value, or the missing marker.
    """
    value = vals.get(key, defval)
    if value is None:
      return f'${key}' if default is None else default

    return value

  return lookup_fn


def attr_lookup(obj, default):
  """Create a lookup function resolving keys as object attributes.

  Args:
    obj: Object whose attributes provide the substitution values.
    default: Default value used when an attribute is missing; when None
      the missing placeholders resolve to '<delim><key>'.

  Returns:
    A lookup function taking (key, defval).
  """

  def lookup_fn(key, defval=None):
    """Resolve a key as an object attribute, falling back to the default.

    Args:
      key: The attribute name to look up.
      defval: Optional default value returned when the attribute is
        missing.

    Returns:
      The attribute value, the default value, or the missing marker.
    """
    value = getattr(obj, key, defval)
    if value is None:
      return f'${key}' if default is None else default

    return value

  return lookup_fn


def template_replace(st, vals=None, lookup_fn=None, delim=None, misses_ok=None):
  """Substitute placeholders in a template string.

  Placeholders use the delimiter (default '$') and may include default
  values: ${ID:DEFAULT_VALUE}.

  Args:
    st: The template string.
    vals: Optional dictionary of substitutions (used if lookup_fn is None).
    lookup_fn: Optional custom lookup function taking (key, defval).
    delim: Optional delimiter character; defaults to '$'.
    misses_ok: If True, unresolved placeholders are left as-is instead of
      raising KeyError.

  Returns:
    The substituted string.

  Example:
    from py_misc_utils import template_replace as tr

    print(tr.template_replace('user=$user', vals={'user': 'ada'}))  # user=ada
    print(tr.template_replace('${user}', vals={'user': 'ada'}))  # ada

    # A per-key default is used when the key is missing:
    print(tr.template_replace('${mode:fine}', vals={}))  # fine

    # A custom delimiter:
    print(tr.template_replace('a=@a@', delim='@', vals={'a': 1}))  # a=1@

    # Unresolved placeholders raise KeyError unless misses_ok is set:
    print(tr.template_replace('$a$b', vals={'a': 1}, misses_ok=True))  # 1$b
  """
  delim = delim or '$'
  misses_ok = False if misses_ok is None else misses_ok

  class Template(string.Template):
    """string.Template subclass accepting ${ID:DEFAULT} placeholders."""

    # Allow for brace ID with the format ${ID:DEFAULT_VALUE}.
    braceidpattern = r'((?a:[_a-z][_a-z0-9]*)(:[^}]*)?)'
    delimiter = delim

  if lookup_fn is None:
    lookup_fn = _dict_lookup_fn(vals, delim, misses_ok)

  return Template(st).safe_substitute(_FnDict(lookup_fn))
