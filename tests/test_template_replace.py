"""Tests for py_misc_utils.template_replace.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): template_replace() substitutes $NAME and ${NAME}
placeholders from a values dict; ${NAME:DEFAULT} applies a per-key
default; a custom delimiter is honored; missing keys raise KeyError
unless misses_ok=True (resolving to the '$<key>' marker); defval_lookup
and attr_lookup provide alternative lookup functions.
"""


from py_misc_utils import template_replace as tr


def test_simple_substitution():
  """$NAME and ${NAME} placeholders both resolve from the dict."""
  assert tr.template_replace('Hello $name!', vals=dict(name='Ada')) == 'Hello Ada!'
  assert tr.template_replace('Hello ${name}!', vals=dict(name='Ada')) == 'Hello Ada!'


def test_adjacent_placeholder_needs_braces():
  """Braces disambiguate placeholders adjacent to word characters."""
  out = tr.template_replace('X-${name}s', vals=dict(name='boby'))
  assert out == 'X-bobys'


def test_default_value():
  """${NAME:DEFAULT} resolves to DEFAULT when the key is missing."""
  out = tr.template_replace('${missing:fallback}', vals=dict())
  assert out == 'fallback'


def test_default_value_not_used_when_present():
  """The stored value wins over the per-key default."""
  out = tr.template_replace('${name:fallback}', vals=dict(name='Ada'))
  assert out == 'Ada'


def test_custom_delimiter():
  """The delimiter is customizable (e.g. '%')."""
  out = tr.template_replace('Hello %name!', delim='%', vals=dict(name='Ada'))
  assert out == 'Hello Ada!'


def test_missing_key_left_in_place():
  """Report-only: safe_substitute leaves unresolved placeholders in
  place - missing keys resolve to the '$<key>' marker even without
  misses_ok=True (the misses_ok=False KeyError path is unreachable via
  the public template_replace API)."""
  assert tr.template_replace('Hello $name!', vals=dict()) == 'Hello $name!'
  assert tr.template_replace('Hello ${name}!', vals=dict()) == 'Hello ${name}!'


def test_missing_key_misses_ok():
  """With misses_ok=True missing keys resolve to the marker '$name'."""
  out = tr.template_replace('Hello $name!', vals=dict(), misses_ok=True)
  assert out == 'Hello $name!'


def test_lookup_fn_defval_lookup():
  """defval_lookup() substitutes from a dict with a global default."""
  lookup = tr.defval_lookup(dict(a='A'), default='?')
  out = tr.template_replace('$a $b', lookup_fn=lookup)
  assert out == 'A ?'


def test_lookup_fn_attr_lookup():
  """attr_lookup() substitutes from object attributes."""

  class Obj:
    alpha = 1

  out = tr.template_replace('a=$alpha', lookup_fn=tr.attr_lookup(Obj(), None))
  assert out == 'a=1'


def test_lookup_fn_attr_default():
  """attr_lookup() applies the fallback default for missing attrs."""
  out = tr.template_replace('a=$missing',
                            lookup_fn=tr.attr_lookup(object(), default='N/A'))
  assert out == 'a=N/A'


def test_lookup_fn_defval_per_key():
  """The per-key default is passed to the custom lookup function."""
  seen = []

  def lookup(key, defval=None):
    seen.append((key, defval))

    return 'X'

  out = tr.template_replace('${a:Z}', lookup_fn=lookup)
  assert out == 'X'
  assert seen == [('a', 'Z')]
