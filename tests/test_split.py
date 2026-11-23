"""Tests for the py_misc_utils.split quote-aware splitting utilities.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): parts come back as a tuple, quote characters
are KEPT in the parts, escaped separators are honored only with
unescape=True, and unquote() strips one matching quote pair.
"""

import pytest

import py_misc_utils.split as sp


def test_split_separates_on_plain_separators():
  """Plain separators split the string at every occurrence."""
  assert sp.split('a,b,c', ',') == ('a', 'b', 'c')


def test_split_keeps_quoted_sections_whole_with_their_quotes():
  """Sections inside quotes are not split; quote characters are kept."""
  assert sp.split('a,"b,c",d', ',') == ('a', '"b,c"', 'd')
  assert sp.split("a,'b c',d", ',') == ('a', "'b c'", 'd')


def test_split_ignores_separators_inside_nesting_pairs():
  """Separators inside quote pairs stay inside a single part."""
  assert sp.split('a,(b,c),d', ',') == ('a', '(b,c)', 'd')
  assert sp.split('a,"(b,c)",d', ',') == ('a', '"(b,c)"', 'd')


def test_split_keeps_escaped_separators_without_unescape():
  """Without unescape, escaped separators stay escaped in the part."""
  assert sp.split('a\\,b,c', ',') == ('a\\,b', 'c')


def test_split_unescapes_separators_on_request():
  """With unescape=True the escape character is removed from the parts."""
  assert sp.split('a\\,b,c', ',', unescape=True) == ('a,b', 'c')


def test_split_of_empty_string_yields_an_empty_tuple():
  """An empty input string yields no parts at all."""
  assert sp.split('', ',') == ()


def test_split_returns_single_part_without_separator():
  """A string without the separator yields one unsplit part."""
  assert sp.split('abc', ',') == ('abc',)


def test_split_keeps_empty_parts_between_separators():
  """Empty parts between separators are preserved."""
  assert sp.split('a,,b', ',') == ('a', '', 'b')


def test_split_supports_bytes_data():
  """bytes data splits the same way strings do."""
  assert sp.split(b'a,b', b',') == (bytearray(b'a'), bytearray(b'b'))


def test_split_accepts_regex_separators():
  """The separator is treated as a regular expression."""
  assert sp.split('a1b22c', r'\d+') == ('a', 'b', 'c')


def test_split_supports_custom_quote_contexts():
  """A custom quote context adds nesting pairs for splitting."""
  assert sp.split('<a,b>,c', ',',
                  quote_ctx=sp.make_context({'<': '>'})) == ('<a,b>', 'c')


def test_split_rejects_unmatched_quotes():
  """Unbalanced quotes make split() fail with an AssertionError."""
  with pytest.raises(AssertionError):
    sp.split('"abc', ',')


def test_split_honors_escaped_quotes_inside_quoted_sections():
  """Escaped quotes do not terminate the quoted section."""
  assert sp.split('"a\\"b",c', ',') == ('"a\\"b"', 'c')


def test_unquote_strips_matching_quote_pairs():
  """unquote() removes one matching surrounding quote pair."""
  assert sp.unquote('"hello"') == 'hello'
  assert sp.unquote("'hi'") == 'hi'
  assert sp.unquote('(nest)') == 'nest'
  assert sp.unquote('[lst]') == 'lst'
  assert sp.unquote('{map}') == 'map'
  assert sp.unquote('<ang>') == 'ang'
  assert sp.unquote('`tick`') == 'tick'


def test_unquote_unescapes_the_quote_character():
  """unquote() unescapes escaped quote characters inside the pair."""
  assert sp.unquote('"a\\"b"') == 'a"b'


def test_unquote_keeps_unquoted_strings():
  """Strings without a matching quote pair are returned unchanged."""
  assert sp.unquote('plain') == 'plain'
  assert sp.unquote('"abc\'') == '"abc\''
