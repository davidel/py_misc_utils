"""Tests for py_misc_utils.global_namespace.

In-process coverage of Var/get/set and the parent/child switch semantics.
One fork-context round-trip is marked slow (spawns a real process).

Isolation rules: the module registry (_NS) is process-global state, so
tests never clear, replace or save/restore it. Every test uses variable
names which are unique across the whole suite, and only mutates entries
it created itself.
"""

import multiprocessing

import pytest

from py_misc_utils import global_namespace as gns
from py_misc_utils import multiprocessing as mp_utils

_NS_KEY = 'py-misc-utils-tests-gns'


def _mkname(test_name):
  """Suite-unique variable name for the given test."""
  return f'{_NS_KEY}-{test_name}'


def test_get_returns_default_on_missing():
  v = gns.Var(_mkname('get-default'), defval={'a': 1})

  assert gns.get(v) == {'a': 1}


def test_get_force_false_missing_returns_none():
  v = gns.Var(_mkname('get-noforce'))

  assert gns.get(v, force=False) is None


def test_set_roundtrip_and_previous_value():
  name = _mkname('set-roundtrip')
  v = gns.Var(name)
  prev = gns.set(v, 'one')

  assert prev is None
  assert gns.get(gns.Var(name)) == 'one'

  prev = gns.set(v, 'two')

  assert prev == 'one'
  assert gns.get(gns.Var(name)) == 'two'


def test_var_name_is_registry_key():
  # Two Var instances sharing the same name address the same entry.
  name = _mkname('registry-key')
  gns.set(gns.Var(name), 42)

  assert gns.get(gns.Var(name)) == 42


def _upper(data):
  return data.upper()


def test_defval_function_called_lazily():
  calls = []

  def _defval():
    calls.append(1)

    return 'lazy'

  # fork_init=True: the local-scope defval callable cannot cross a
  # process boundary, and this test is about lazy evaluation, not about
  # switch propagation - the same idiom the library itself uses for
  # lambda defvals.
  v = gns.Var(_mkname('defval-lazy'), defval=_defval, fork_init=True)

  assert gns.get(v) == 'lazy'
  assert len(calls) == 1

  # Already initialized: no further defval invocations.
  gns.get(v)
  assert len(calls) == 1


def test_parent_switch_serializes_state():
  name = _mkname('parent-switch')
  v = gns.Var(name, parent_fn=_upper)
  gns.set(v, 'state')

  pns = gns.parent_switch('spawn')

  assert name in pns
  assert pns[name].data == 'STATE'


def test_fork_init_vars_excluded_from_switches():
  name_keep = _mkname('fork-keep')
  name_reinit = _mkname('fork-reinit')
  gns.set(gns.Var(name_keep), 'carry-me')
  gns.set(gns.Var(name_reinit, fork_init=True), 'drop-me')

  pns = gns.parent_switch('spawn')

  assert name_keep in pns
  assert name_reinit not in pns


@pytest.mark.slow
@pytest.mark.legacy_fork
def test_fork_child_filters_fork_init_vars():
  # _child_fork() runs in the forked child via os.register_at_fork():
  # fork_init=False variables carry the parent value over, while
  # fork_init=True ones are dropped (fresh-initialized in the child).
  # The registry is only mutated inside the disposable child process.
  name_keep = _mkname('fork-child-keep')
  name_reinit = _mkname('fork-child-reinit')
  v_keep = gns.Var(name_keep)
  gns.set(v_keep, 'keep')
  v_reinit = gns.Var(name_reinit, fork_init=True, defval=lambda: 'reinit')
  gns.set(v_reinit, 'drop')

  ctx = multiprocessing.get_context('fork')
  q = ctx.Queue()
  p = ctx.Process(target=_child_checks_fork_filter, args=(v_keep, v_reinit, q))
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)

  assert p.exitcode == 0
  assert got == ['keep', 'reinit']


def _child_checks_fork_filter(v_keep, v_reinit, q):
  # Runs in the forked child: _child_fork() has already filtered _NS.
  # fork_init=False variables carry the parent value over, while
  # fork_init=True ones are dropped and get fresh-initialized (defval)
  # on the first forced get().
  kept = gns.get(v_keep, force=False)
  reinit = gns.get(v_reinit)
  q.put([kept, reinit])


def test_parent_switch_unknown_method_asserts():
  _v = gns.Var(_mkname('bad-method'))

  with pytest.raises(AssertionError):
    gns.parent_switch('not-a-method')


@pytest.mark.slow
@pytest.mark.legacy_fork
def test_fork_roundtrip():
  name = _mkname('fork-roundtrip')
  v = gns.Var(name)
  gns.set(v, {'k': 'parent-value'})
  ctx = multiprocessing.get_context('fork')
  q = ctx.Queue()
  p = ctx.Process(target=_child_gets, args=(v, q))
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)

  assert p.exitcode == 0
  assert got == {'k': 'parent-value'}


def _child_gets(v, q):
  # In fork mode the child inherits the parent namespace.
  q.put(gns.get(v))


@pytest.mark.slow
def test_spawn_roundtrip_propagates_parent_state():
  """parent_switch/child_switch carry GNS state across spawn boundaries."""
  name = _mkname('spawn-roundtrip')
  v = gns.Var(name)
  gns.set(v, {'k': 'parent-value'})
  ctx = multiprocessing.get_context('spawn')
  q = ctx.Queue()
  p = mp_utils.create_process(_child_gets, args=(v, q), context='spawn')
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)

  assert p.exitcode == 0
  assert got == {'k': 'parent-value'}
