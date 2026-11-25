"""Tests for py_misc_utils.dynamod.

Expectations pinned to the observed behavior: create_module() writes a
source file into a temporary module folder and returns the module;
get_module() imports it by name; module_name() builds the importable
name with a dot separator.
"""

from py_misc_utils import dynamod as dm


def test_create_and_get_module():
  """create_module() creates and get_module() imports it back."""
  code = "ANSWER = 42\n"
  name = 'dyn_test_mod_42'
  mod = dm.create_module(name, code)
  assert mod.ANSWER == 42

  imported = dm.get_module(name)
  assert imported.ANSWER == 42


def test_module_name_dot_separator():
  """module_name() builds the full importable name."""
  assert dm.module_name('foo') == f'{dm._MODNAME}.foo'
  assert dm.module_name('a.b') == f'{dm._MODNAME}.a.b'
