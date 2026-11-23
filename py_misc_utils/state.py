"""Serializable object state persistence utilities.

This module provides the :class:`StateBase` mixin which allows subclasses to
save/restore their state via _get_state/_set_state hooks, plus :func:`to_state`
and :func:`from_state` functions for pickling state to files. Subclasses can
contribute additional state via _store_state/_load_state keyed by class.
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
  """
  with gfs.open(path, mode='rb') as sfd:
    state = pickle.load(sfd)

  state.update(kwargs)

  obj = cls.__new__(cls)
  obj._set_state(state)

  return obj
