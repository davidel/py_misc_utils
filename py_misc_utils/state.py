import pickle

from . import gfs


_STATE_KEY = '__SB_STATE__'


def _kname(cls, name):
  return f'{cls.__name__}.{name}'


class StateBase:

  def _get_state(self, state):
    return state

  def _set_state(self, state):
    self.__dict__.update(state)

  def _store_state(self, cls, **kwargs):
    sdict = getattr(self, _STATE_KEY, None)
    if sdict is None:
      sdict = dict()
      setattr(self, _STATE_KEY, sdict)

    for k, v in kwargs.items():
      sdict[_kname(cls, k)] = v

  def _load_state(self, cls, state, name, defval=None):
    sdict = state.get(_STATE_KEY)

    return sdict.get(_kname(cls, name), defval) if sdict is not None else defval


def to_state(obj, path):
  # Needs a copy here, as the _get_state() call chains will modify the state.
  state = obj._get_state(obj.__dict__.copy())
  with gfs.open(path, mode='wb') as sfd:
    pickle.dump(state, sfd)


def from_state(cls, path, **kwargs):
  with gfs.open(path, mode='rb') as sfd:
    state = pickle.load(sfd)

  state.update(kwargs)

  obj = cls.__new__(cls)
  obj._set_state(state)

  return obj

