"""Tests for py_misc_utils.remap_pickle.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): the Unpickler resolves class references applying
remaps (remapped names bypass the safe_refs whitelist check) and
raising RuntimeError for references outside safe_refs; loads()/load()
accept remaps/safe_refs kwargs; make_module() builds a pickle-module
clone whose load/loads apply the given defaults.
"""

import pickle

import pytest

from py_misc_utils import remap_pickle as rp


def test_plain_roundtrip():
  """Data pickled with plain pickle loads back unchanged."""
  data = pickle.dumps(dict(a=[1, 2, 3]))
  assert rp.loads(data) == dict(a=[1, 2, 3])


def test_load_from_file(tmp_path):
  """load() reads from a file object."""
  path = tmp_path / 'data.pkl'
  with open(path, 'wb') as fpt:
    pickle.dump('payload', fpt)

  with open(path, 'rb') as fpt:
    assert rp.load(fpt) == 'payload'


class Local:
  """A picklable-by-reference class from this test module."""

  def __init__(self, v=1):
    self.v = v


class Other:
  pass


def test_safe_refs_allows():
  """References within the whitelist load fine."""
  data = pickle.dumps(dict(a=1))
  res = rp.loads(data, safe_refs={'builtins.dict', 'builtins.int'})
  assert res == dict(a=1)


def test_safe_refs_rejects():
  """A custom class reference outside the whitelist raises RuntimeError.

  Note: builtin container opcodes (list/dict/tuple) never invoke
  find_class, so safe_refs only restricts real class references.
  """
  data = pickle.dumps(Local())
  with pytest.raises(RuntimeError):
    rp.loads(data, safe_refs={'builtins.dict'})


def test_safe_refs_allows_custom():
  """A whitelisted custom class reference loads fine."""
  data = pickle.dumps(Local())
  res = rp.loads(data, safe_refs={f'{__name__}.Local'})
  assert isinstance(res, Local)


def test_remap_rewrites_custom_reference():
  """A remap rewrites a custom class reference to another path."""
  data = pickle.dumps(Local(5))
  res = rp.loads(data, remaps={f'{__name__}.Local': f'{__name__}.Other'})
  assert type(res) is Other


def test_remap_bypasses_safe_refs():
  """A CHANGED remapped name skips the safe_refs check (only remaps
  actually renaming the reference skip the whitelist)."""
  data = pickle.dumps(Local())
  res = rp.loads(data,
                 remaps={f'{__name__}.Local': f'{__name__}.Other'},
                 safe_refs={'builtins.dict'})
  assert type(res) is Other


def test_make_module():
  """make_module() builds a pickle clone with default kwargs applied."""
  module = rp.make_module(safe_refs={'builtins.dict', f'{__name__}.Local'})

  data = pickle.dumps(dict(a=1))
  assert module.loads(data) == dict(a=1)
  assert module.loads(pickle.dumps(Local())).v == 1

  with pytest.raises(RuntimeError):
    module.loads(pickle.dumps(Other()))
