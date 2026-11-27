"""Serializable object state persistence utilities.

This module provides the :class:`StateBase` mixin which allows subclasses to
save/restore their state via _get_state/_set_state hooks, plus :func:`to_state`
and :func:`from_state` functions for pickling state to files. Subclasses can
contribute additional state via _store_state/_load_state keyed by class.

Example:
  import os
  import tempfile

  import py_misc_utils.state as st


  class Session(st.StateBase):

    def __init__(self, user):
      self.user = user


  tdir = tempfile.mkdtemp()
  st.to_state(Session('alice'), os.path.join(tdir, 'session.state'))
  sess = st.from_state(Session, os.path.join(tdir, 'session.state'))
  assert sess.user == 'alice'
"""

import pickle

from . import gfs

_STATE_KEY = '__SB_STATE__'


def _kname(cls, name):
  """Build a namespaced state key for a class attribute.

  Args:
    cls: The class contributing the state.
    name: The attribute name.

  Returns:
    A string of the form '<ClassName>.<name>'.
  """
  return f'{cls.__name__}.{name}'


class StateBase:
  """Mixin providing pickle-based state save/restore for subclasses.

  Subclasses override _get_state/_set_state to customize serialization and
  use _store_state/_load_state to contribute class-namespaced extra state.

  Example:
    import py_misc_utils.state as st


    class Session(st.StateBase):

      def __init__(self, user, token):
        self.user = user
        self.token = token

      def _get_state(self, state):
        state['user'] = self.user
        state['token_len'] = len(self.token)
        return state

      def _set_state(self, state):
        self.user = state['user']
        self.token = '*' * state['token_len']


    sess = Session('alice', 'secret')
    state = sess._get_state({})
    assert state == {'user': 'alice', 'token_len': 6}, state

    sess2 = Session.__new__(Session)
    sess2._set_state(state)
    assert sess2.user == 'alice' and sess2.token == '******'

  Note:
    State entries contributed via _store_state live in the __SB_STATE__
    instance attribute, which is not part of the state dictionary
    returned by the default _get_state. For such entries to be persisted,
    either call _store_state before to_state() serializes the object, or
    override _get_state to merge the __SB_STATE__ contents (e.g. with
    state.update(self.__dict__)) into the returned dictionary.
  """

  def _get_state(self, state):
    """Return the state dictionary to persist (default: passthrough).

    Args:
      state: The raw __dict__ copy.

    Returns:
      The state dictionary to store.
    """
    return state

  def _set_state(self, state):
    """Restore object state from a state dictionary (default: update __dict__).

    Args:
      state: The state dictionary to apply.
    """
    self.__dict__.update(state)

  def _store_state(self, cls, **kwargs):
    """Store class-namespaced state entries.

    Args:
      cls: The class contributing the state.
      **kwargs: Attribute name/value pairs to store.

    Example:
      import py_misc_utils.state as st


      class Base(st.StateBase):

        def __init__(self, v):
          self.v = v


      class Child(Base):

        def __init__(self, v, extra):
          super().__init__(v)
          self._store_state(Child, extra=extra)


      c = Child(4, 'E')
      assert c.__dict__['__SB_STATE__'] == {'Child.extra': 'E'}, c.__dict__
      # Class-namespaced entries are read back with the matching class.
      assert c._load_state(Child, c.__dict__, 'extra') == 'E'
      assert c._load_state(Base, c.__dict__, 'extra', 'MISS') == 'MISS'
    """
    sdict = getattr(self, _STATE_KEY, None)
    if sdict is None:
      sdict = dict()
      setattr(self, _STATE_KEY, sdict)

    for k, v in kwargs.items():
      sdict[_kname(cls, k)] = v

  def _load_state(self, cls, state, name, defval=None):
    """Load a class-namespaced state entry.

    Args:
      cls: The class that contributed the state.
      state: The loaded state dictionary.
      name: The attribute name to load.
      defval: Optional default value if the key is missing.

    Returns:
      The stored value or defval.
    """
    sdict = state.get(_STATE_KEY)

    return sdict.get(_kname(cls, name), defval) if sdict is not None else defval


def to_state(obj, path):
  """Persist an object's state to a file via pickle.

  Args:
    obj: The StateBase-derived object to serialize.
    path: Filesystem path to write the state to.

  Example:
    import os
    import tempfile

    import py_misc_utils.state as st


    class Session(st.StateBase):

      def __init__(self, user, retries=3):
        self.user = user
        self.retries = retries


    tdir = tempfile.mkdtemp()
    st.to_state(Session('bob'), os.path.join(tdir, 'sess.state'))
    assert st.from_state(Session, os.path.join(tdir, 'sess.state')).user == 'bob'
  """
  # Needs a copy here, as the _get_state() call chains will modify the state.
  state = obj._get_state(obj.__dict__.copy())
  with gfs.open(path, mode='wb') as sfd:
    pickle.dump(state, sfd)


def from_state(cls, path, **kwargs):
  """Restore an object of cls from persisted state, overriding with kwargs.

  Args:
    cls: The class to instantiate.
    path: Filesystem path to read the state from.
    **kwargs: Overrides applied to the loaded state before restore.

  Returns:
    A new instance of cls with its state restored.

  Example:
    import os
    import tempfile

    import py_misc_utils.state as st


    class Session(st.StateBase):

      def __init__(self, user, retries=3):
        self.user = user
        self.retries = retries


    tdir = tempfile.mkdtemp()
    st.to_state(Session('bob', retries=5), os.path.join(tdir, 'sess.state'))
    # kwargs override the persisted top-level state entries.
    sess = st.from_state(Session, os.path.join(tdir, 'sess.state'), retries=1)
    assert (sess.user, sess.retries) == ('bob', 1), (sess.user, sess.retries)
  """
  with gfs.open(path, mode='rb') as sfd:
    state = pickle.load(sfd)

  state.update(kwargs)

  obj = cls.__new__(cls)
  obj._set_state(state)

  return obj
