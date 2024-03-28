import pickle


class StateBase:

  def _get_state(self, state):
    return state.copy()

  def _set_state(self, state):
    self.__dict__.update(state)


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

