"""Tests for py_misc_utils.dyn_modules.

Exercises DynLoader discovery from a filesystem path and from a package
module name, postfix filtering, sorted load order and the MODULE_NAME
override mechanism.
"""

import os
import sys

import pytest

from py_misc_utils import dyn_modules as dym
from py_misc_utils import module_utils as mu


def _mkmod(path, name, code):
  with open(os.path.join(path, name + '.py'), 'w') as fpt:
    fpt.write(code)


def _sweep(names):
  """Remove DynLoader-installed modules from sys.modules."""
  for name in names:
    sys.modules.pop(name, None)


def test_load_from_path_basic(temp_path):
  _mkmod(temp_path, 'alpha', 'VALUE = 10\n')
  _mkmod(temp_path, 'beta', 'VALUE = 20\n')

  loader = dym.DynLoader(path=temp_path)
  try:
    assert loader.module_names() == ('alpha', 'beta')
    assert loader.modules() == (loader.get('alpha'), loader.get('beta'))
    assert loader['alpha'].VALUE == 10
    assert loader['beta'].VALUE == 20
    assert len(loader) == 2
  finally:
    _sweep(['alpha', 'beta'])


def test_path_mode_installs_under_derived_name(temp_path):
  # DynLoader passes modname= to load_module(), which installs modules
  # into sys.modules under their derived names: pinned public behavior.
  _mkmod(temp_path, 'hermit', 'X = 1\n')
  loader = dym.DynLoader(path=temp_path)

  try:
    assert loader.get('hermit') is not None
    assert 'hermit' in loader.module_names()
    assert 'hermit' in sys.modules
  finally:
    _sweep(['hermit'])


def test_non_py_files_are_skipped(temp_path):
  _mkmod(temp_path, 'keeper', 'V = 1\n')
  with open(os.path.join(temp_path, 'notes.txt'), 'w') as fpt:
    fpt.write('not a module\n')

  loader = dym.DynLoader(path=temp_path)
  assert loader.module_names() == ('keeper',)


def test_postfix_filters_and_strips_names(temp_path):
  _mkmod(temp_path, 'svc', 'V = 1\n')  # no postfix: skipped
  _mkmod(temp_path, 'svc_plugin', 'V = 2\n')
  _mkmod(temp_path, 'other_plugin', 'V = 3\n')

  loader = dym.DynLoader(path=temp_path, postfix='_plugin')
  try:
    assert loader.module_names() == ('other', 'svc')
    assert loader.get('svc').V == 2
    assert loader.get('other').V == 3
    assert loader.get('missing') is None
  finally:
    _sweep(['svc_plugin', 'other_plugin'])


def test_sorted_load_order(temp_path):
  # Modules are loaded in sorted-name order: prove it via the order in
  # which each module body appends to a trace file at import time.
  trace = os.path.join(temp_path, 'trace.txt')
  for name in ('zulu', 'alpha', 'mike'):
    _mkmod(temp_path, name, f'open({trace!r}, "a").write("{name}\\n")\n')

  loader = dym.DynLoader(path=temp_path)
  try:
    assert loader.module_names() == ('alpha', 'mike', 'zulu')

    with open(trace) as fpt:
      assert fpt.read().splitlines() == ['alpha', 'mike', 'zulu']
  finally:
    _sweep(['zulu', 'alpha', 'mike'])


def test_module_name_override(temp_path):
  _mkmod(temp_path, 'plain_file_name', 'MODULE_NAME = "pretty_name"\n')

  loader = dym.DynLoader(path=temp_path)
  try:
    assert loader.module_names() == ('pretty_name',)
    assert loader.get('plain_file_name') is None
    assert loader.get('pretty_name') is not None
  finally:
    _sweep(['plain_file_name'])


def test_getitem_raises_keyerror_on_missing(temp_path):
  _mkmod(temp_path, 'present', 'V = 1\n')
  loader = dym.DynLoader(path=temp_path)
  try:
    assert loader.get('absent') is None
    with pytest.raises(KeyError):
      loader['absent']
  finally:
    _sweep(['present'])


def test_syntax_error_module_fails_at_construction(temp_path):
  _mkmod(temp_path, 'broken', 'this is not python(\n')
  try:
    with pytest.raises(SyntaxError):
      dym.DynLoader(path=temp_path)
  finally:
    # install_module() runs before exec_module() in load_module(), so the
    # failed module still lands in sys.modules: sweep it.
    _sweep(['broken'])


def test_modname_mode_loads_package_children(temp_path):
  # Build a real package: dynpkg/__init__.py + children, on sys.path.
  pkgdir = os.path.join(temp_path, 'dynpkg')
  os.makedirs(pkgdir)
  with open(os.path.join(pkgdir, '__init__.py'), 'w') as fpt:
    fpt.write('\n')
  _mkmod(pkgdir, 'plug_x', 'VALUE = 10\n')
  _mkmod(pkgdir, 'plug_y', 'VALUE = 20\n')

  mu.add_sys_path(temp_path)
  try:
    loader = dym.DynLoader(modname='dynpkg')
    assert loader.module_names() == ('__init__', 'plug_x', 'plug_y')
    assert loader.get('plug_x').VALUE == 10
    assert loader.get('plug_y').VALUE == 20
  finally:
    sys.path.remove(temp_path)
    _sweep([
        name for name in sys.modules if name == 'dynpkg' or name.startswith('dynpkg.')
    ])


def test_both_modname_and_path_rejected(temp_path):
  _mkmod(temp_path, 'only_one', 'V = 1\n')
  with pytest.raises(AssertionError):
    dym.DynLoader(modname='dynpkg', path=temp_path)


def test_neither_modname_nor_path_rejected():
  with pytest.raises(AssertionError):
    dym.DynLoader()
