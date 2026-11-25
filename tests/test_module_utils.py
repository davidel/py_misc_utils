"""Tests for py_misc_utils.module_utils.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): split_module_name() splits at the last dot;
load_module() imports a module from a filesystem path (optionally
installing it into sys.modules); import_module() dispatches names vs
paths; module_getter()/import_module_names() enumerate importable
modules; clone_module() re-executes a module spec into a fresh module;
module_file() returns __file__ or None.
"""

import os
import sys

from py_misc_utils import module_utils as mu


def test_split_module_name():
  """The name is split at the last dot; a dotless name has no parent."""
  assert mu.split_module_name('a.b.c') == ('a.b', 'c')
  assert mu.split_module_name('single') == ('', 'single')


def test_load_module_from_path(temp_path):
  """A module can be loaded from a filesystem .py path."""
  modfile = os.path.join(temp_path, 'mymod.py')
  with open(modfile, 'w') as fd:
    fd.write('VALUE = 42\n')

  module = mu.load_module(modfile, modname='mymod_test')

  assert module.VALUE == 42
  assert sys.modules.get('mymod_test') is module


def test_load_module_install_is_always_true(temp_path):
  """Report-only bug: load_module() coerces install with
  'install or True', so install=False still installs the module into
  sys.modules."""
  modfile = os.path.join(temp_path, 'plainmod.py')
  with open(modfile, 'w') as fd:
    fd.write('X = 1\n')

  module = mu.load_module(modfile, modname='plainmod_test', install=False)

  assert module.X == 1
  assert sys.modules.get('plainmod_test') is module


def test_import_module_by_name():
  """import_module() delegates to importlib for plain names."""
  module = mu.import_module('json')
  assert module.dumps({}) == '{}'


def test_import_module_by_path(temp_path):
  """import_module() loads a file path via load_module."""
  modfile = os.path.join(temp_path, 'bypath.py')
  with open(modfile, 'w') as fd:
    fd.write('MARK = \"p\"\n')

  module = mu.import_module(modfile, modname='bypath_test')

  assert module.MARK == 'p'


def test_maybe_import_existing():
  """maybe_import() returns the module when present."""
  assert mu.maybe_import('json').dumps({}) == '{}'


def test_maybe_import_missing():
  """maybe_import() returns None for a missing module."""
  assert mu.maybe_import('no_such_module_abc123') is None


def test_module_file():
  """module_file() returns __file__ for real modules, None otherwise."""
  assert mu.module_file(sys) is None
  json_module = mu.import_module('json')
  assert mu.module_file(json_module) is not None


def test_clone_module():
  """clone_module() re-executes the spec into a fresh module object."""
  first = mu.import_module('textwrap')
  second = mu.clone_module('textwrap')

  assert second is not first
  assert second.wrap is not None


def test_add_sys_path(temp_path):
  """add_sys_path() appends a new directory to sys.path."""
  before = list(sys.path)
  try:
    mu.add_sys_path(temp_path)
    assert temp_path in sys.path
  finally:
    sys.path = before


def test_install_and_get_module(temp_path):
  """install_module() registers a module under the given name."""
  modfile = os.path.join(temp_path, 'instmod.py')
  with open(modfile, 'w') as fd:
    fd.write('K = 9\n')

  module = mu.load_module(modfile, install=False)
  mu.install_module('instmod_test', module)

  assert sys.modules['instmod_test'] is module
