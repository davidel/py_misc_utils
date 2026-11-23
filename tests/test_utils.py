"""Tests for the py_misc_utils.utils string, env and value helpers.

Expectations follow the documented API contract: formatting preserves the
input sequence type, splits drop empty parts and trim separators, env
lookups honor defaults and type conversion, and value_or only substitutes
genuinely-None values.
"""

import py_misc_utils.utils as pyu


def test_format_formats_elements_and_preserves_sequence_type():
  """format() applies the format spec element-wise and keeps the type."""
  assert pyu.format([1, 2, 3], '05d') == ['00001', '00002', '00003']
  assert pyu.format((1, 2), 'x') == ('1', '2')
  assert pyu.format([1.5], '.2f') == ['1.50']


def test_ws_split_splits_on_whitespace_runs():
  """ws_split() splits on whitespace runs and returns a tuple of parts."""
  assert pyu.ws_split('a b\tc\nd  e') == ('a', 'b', 'c', 'd', 'e')
  # Empty parts between separators are preserved (re.split semantics).
  assert pyu.ws_split('   ') == ('', '')


def test_comma_split_trims_whitespace_around_commas():
  """comma_split() trims parts, drops empty ones and returns a tuple."""
  assert pyu.comma_split('a, b ,c') == ('a', 'b', 'c')
  # Empty parts between separators are preserved (re.split semantics).
  assert pyu.comma_split('a,,b') == ('a', '', 'b')


def test_resplit_treats_the_separator_as_regex():
  """resplit() splits on a regex separator and returns a tuple."""
  assert pyu.resplit('a;b;c', ';') == ('a', 'b', 'c')
  assert pyu.resplit('a1b22c', r'\d+') == ('a', 'b', 'c')


def test_env_returns_default_when_variable_is_unset(monkeypatch):
  """env() returns the default value for unset variables."""
  monkeypatch.delenv('PYMI_TEST_VAR', raising=False)

  assert pyu.env('PYMI_TEST_VAR', 7, vtype=int) == 7


def test_env_converts_set_values(monkeypatch):
  """env() converts set values to the requested type, else keeps strings."""
  monkeypatch.setenv('PYMI_TEST_PORT', '8080')

  assert pyu.env('PYMI_TEST_PORT', 0, vtype=int) == 8080
  assert pyu.env('PYMI_TEST_PORT', '0') == '8080'


def test_value_or_substitutes_only_none_values():
  """value_or() returns defval only for None; falsy values pass through."""
  assert pyu.value_or(None, 5) == 5
  assert pyu.value_or(0, 5) == 0
  assert pyu.value_or('', 'x') == ''
  assert pyu.value_or(False, True) is False
