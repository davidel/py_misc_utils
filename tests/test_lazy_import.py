"""Tests for py_misc_utils.lazy_import.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): lazy_import() returns a module wrapped by
importlib.util.LazyLoader - the module instance is registered in
sys.modules under its name and is a _LazyModule until the first
attribute access, at which point the real module body executes and the
instance class mutates to module. A missing module spec raises
AttributeError on creation (report-only: no guard on spec=None).
"""

import sys

import pytest

from py_misc_utils import lazy_import as li


def test_lazy_import_defers_execution(tmp_path, monkeypatch):
  """The module body does not run until an attribute is accessed."""
  modfile = tmp_path / 'lazyslow'
  modfile.mkdir()
  (modfile / '__init__.py').write_text('FLAG = True\n')

  monkeypatch.syspath_prepend(str(tmp_path))
  monkeypatch.delitem(sys.modules, 'lazyslow', raising=False)

  module = li.lazy_import('lazyslow')

  assert sys.modules['lazyslow'] is module
  assert type(module).__name__ == '_LazyModule'

  assert module.FLAG is True
  assert type(module).__name__ == 'module'


def test_lazy_import_stdlib():
  """A stdlib module can be lazily imported and used."""
  module = li.lazy_import('json')
  assert module.dumps({'a': 1}) == '{"a": 1}'


def test_lazy_import_missing_module():
  """A missing module spec is not guarded - AttributeError is raised
  (report-only: no helpful ImportError)."""
  with pytest.raises(AttributeError):
    li.lazy_import('definitely_missing_module_xyz')
