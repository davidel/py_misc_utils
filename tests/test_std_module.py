"""Tests for py_misc_utils.std_module.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): is_std_module() resolves the root module of a dotted
name, imports it, and classifies it by comparing its origin library path
against known stdlib paths; built-ins (no origin) count as stdlib;
results are cached per root module name.
"""


from py_misc_utils import std_module as sm


def test_stdlib_module():
  """Plain stdlib modules are classified as stdlib."""
  assert sm.is_std_module('os') is True
  assert sm.is_std_module('collections.abc') is True


def test_third_party_module():
  """Third-party modules are not stdlib."""
  assert sm.is_std_module('numpy') is False


def test_local_module():
  """The tested package itself is not stdlib."""
  assert sm.is_std_module('py_misc_utils') is False


def test_dotted_name_uses_root():
  """Dotted names are reduced to the root module before classification."""
  assert sm.is_std_module('os.path') == sm.is_std_module('os')
  assert sm.is_std_module('py_misc_utils.gfs') == sm.is_std_module('py_misc_utils')


def test_builtin_counts_as_stdlib():
  """Modules without an origin (built-ins) are treated as stdlib."""
  assert sm.is_std_module('sys') is True


def test_caching_is_per_name():
  """functools.cache returns the same result for repeated lookups."""
  first = sm.is_std_module('random')
  second = sm.is_std_module('random')
  assert first == second
  assert first is True
