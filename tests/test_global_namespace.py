"""Tests for py_misc_utils.global_namespace.

In-process coverage of Var/get/set and the parent/child switch semantics.
One fork-context round-trip is marked slow (spawns a real process).
"""

import multiprocessing

import pytest

from py_misc_utils import global_namespace as gns


@pytest.fixture(autouse=True)
def clean_ns():
  # _NS is a module-global: save/restore it around every test to prevent
  # cross-test pollution.
  saved = dict(gns._NS)
  yield
  gns._NS.clear()
  gns._NS.update(saved)


def test_var_defaults():
  v = gns.Var('x')
  assert v.name == 'x'
  assert v.parent_fn is None
  assert v.child_fn is None
  assert v.fork_init is False
  assert v.defval is None


def test_get_default_value():
  v = gns.Var('x', defval=42)
  assert gns.get(v) == 42


def test_get_callable_defval():
  v = gns.Var('x', defval=lambda: 'dflt')
  assert gns.get(v) == 'dflt'


def test_get_force_false_missing():
  v = gns.Var('x')
  assert gns.get(v, force=False) is None


def test_set_and_get():
  v = gns.Var('x')
  assert gns.set(v, 42) is None
  assert gns.get(v) == 42


def test_set_returns_previous():
  v = gns.Var('x')
  gns.set(v, 1)
  assert gns.set(v, 2) == 1


def test_parent_switch_drops_fork_init(clean_ns):
  v = gns.Var('keep')
  vf = gns.Var('reinit', fork_init=True)
  gns.set(v, 'kept')
  gns.set(vf, [1, 2, 3])
  pns = gns.parent_switch('fork')
  assert 'keep' in pns
  assert 'reinit' not in pns
  assert pns['keep'].data == 'kept'


def test_child_switch_applies_child_fn(clean_ns):
  v = gns.Var('x', child_fn=lambda d: d * 2)
  gns.set(v, 21)
  pns = gns.parent_switch('fork')
  gns.child_switch('fork', pns)
  assert gns.get(gns.Var('x')) == 42


def test_child_switch_replaces_ns(clean_ns):
  v1 = gns.Var('a')
  v2 = gns.Var('b')
  gns.set(v1, 1)
  gns.set(v2, 2)
  pns = gns.parent_switch('fork')
  gns.child_switch('fork', pns)
  # The namespace now holds only the switched variables.
  assert set(gns._NS.keys()) == {'a', 'b'}


def test_bad_method_asserts(clean_ns):
  with pytest.raises(AssertionError):
    gns.parent_switch('badmethod')


@pytest.mark.slow
def test_fork_roundtrip(clean_ns):
  v = gns.Var('shared')
  gns.set(v, {'k': 'parent-value'})
  q = multiprocessing.get_context('fork').Queue()
  p = multiprocessing.get_context('fork').Process(
      target=_child_gets,
      args=(v, q),
  )
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == {'k': 'parent-value'}


def _child_gets(v, q):
  # In fork mode the child inherits the parent namespace.
  q.put(gns.get(v))
