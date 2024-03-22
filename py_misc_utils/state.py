import pickle


ARGS_FIELDS = 'ARGS_FIELDS'
KWARGS_FIELDS = 'KWARGS_FIELDS'
STATE_FIELDS = 'STATE_FIELDS'


def get_state(obj):
  cls = obj.__class__
  fields = []
  for fn in (ARGS_FIELDS, KWARGS_FIELDS, STATE_FIELDS):
    fields.extend(list(getattr(cls, fn, [])))

  return {n: getattr(obj, n, None) for n in fields}


def to_state(obj, path):
  state = get_state(obj)
  with open(path, mode='wb') as sfd:
    pickle.dump(state, sfd)


def from_state(cls, path, *args, **kwargs):
  with open(path, mode='rb') as sfd:
    state = pickle.load(sfd)

  skwargs = {n: getattr(state, n, None) for n in getattr(cls, KWARGS_FIELDS, [])}
  skwargs.update(kwargs)

  sargs = list(args) + [state[an] for an in getattr(cls, ARGS_FIELDS, [])]

  obj = cls(*sargs, **skwargs)

  for sn in getattr(cls, STATE_FIELDS, []):
    setattr(obj, sn, state[sn])

  return obj

