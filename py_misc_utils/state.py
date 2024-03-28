import pickle


_STATE_KEY = '_StateBase_STATE'


class StateBase:

  def _get_state(self, state):
    return state.copy()

  def _set_state(self, state):
    self.__dict__.update(state)

  def _store_state(self, **kwargs):
    sdict = getattr(self, _STATE_KEY, None)
    if sdict is None:
      sdict = dict()
      setattr(self, _STATE_KEY, sdict)

    sdict.update(kwargs)


def fetch(state, name):
  sdict = state.get(_STATE_KEY, None)

  return sdict.get(name, None) if sdict is not None else None


def to_state(obj, path):
  state = obj._get_state(obj.__dict__.copy())
  with open(path, mode='wb') as sfd:
    pickle.dump(state, sfd)


def from_state(cls, path, **kwargs):
  with open(path, mode='rb') as sfd:
    state = pickle.load(sfd)

  state.update(kwargs)

  obj = cls.__new__(cls)
  obj._set_state(state)

  return obj

