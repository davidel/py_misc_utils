"""Tests for py_misc_utils.state to_state/from_state roundtrips.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): to_state() pickles the object's __dict__
through _get_state and from_state() rebuilds the object via
__new__/_set_state; kwargs act as overrides on the top-level state
keys only; _store_state/_load_state namespace entries by contributing
class name (__SB_STATE__ payloads roundtrip when stored before save).
"""

from py_misc_utils import state as st


class Simple(st.StateBase):
  """Minimal stateful object with explicit restore."""

  def __init__(self, a, b):
    self.a = a
    self.b = b

  def _set_state(self, state):
    self.a = state['a']
    self.b = state['b']


class Child(st.StateBase):
  """Object contributing class-namespaced state entries."""

  def __init__(self, base, extra):
    self.base = base
    self.extra = extra
    self._store_state(st.StateBase, base=base)
    self._store_state(Child, extra=extra)

  def _set_state(self, state):
    self.base = self._load_state(st.StateBase, state, 'base')
    self.extra = self._load_state(Child, state, 'extra')


def test_roundtrip_simple_state(tmp_cwd):
  """A simple object roundtrips its attributes through a file."""
  obj = Simple(1, 'x')
  st.to_state(obj, 'state.pkl')
  obj2 = st.from_state(Simple, 'state.pkl')
  assert (obj2.a, obj2.b) == (1, 'x')


def test_from_state_kwargs_override_top_level_keys(tmp_cwd):
  """kwargs replace top-level state entries before _set_state."""
  obj = Simple(1, 'x')
  st.to_state(obj, 'state.pkl')
  obj2 = st.from_state(Simple, 'state.pkl', a=99)
  assert obj2.a == 99
  assert obj2.b == 'x'


def test_namespaced_state_roundtrips(tmp_cwd):
  """_store_state payloads survive persistence and reload."""
  obj = Child('p', 'e')
  st.to_state(obj, 'state.pkl')
  obj2 = st.from_state(Child, 'state.pkl')
  assert (obj2.base, obj2.extra) == ('p', 'e')


def test_namespaced_state_not_overridable_via_kwargs(tmp_cwd):
  """Report-only: kwargs only override top-level state keys, so
  namespaced entries stay at their persisted values."""
  obj = Child('p', 'e')
  st.to_state(obj, 'state.pkl')
  obj2 = st.from_state(Child, 'state.pkl', base='OVR')
  assert obj2.base == 'p'


def test_store_state_uses_class_qualified_keys(tmp_cwd):
  """Entries are keyed as '<ClassName>.<name>' inside __SB_STATE__."""
  obj = Child('p', 'e')
  sdict = obj.__dict__['__SB_STATE__']
  assert sdict == {'StateBase.base': 'p', 'Child.extra': 'e'}


def test_default_get_state_is_passthrough():
  """The default _get_state returns the given state dict unchanged."""
  obj = Simple(1, 'x')
  state = {'a': 1}
  assert obj._get_state(state) is state


def test_default_set_state_updates_dict():
  """The default _set_state merges state into the instance dict."""
  obj = st.StateBase()
  obj._set_state({'z': 5})
  assert obj.z == 5
