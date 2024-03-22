import pickle


ARGS_FIELDS = 'ARGS_FIELDS'
KWARGS_FIELDS = 'KWARGS_FIELDS'
STATE_FIELDS = 'STATE_FIELDS'


def get_state(obj):
  cls = obj.__class__
  fields = []
  for fn in (ARGS_FIELDS, KWARGS_FIELDS, STATE_FIELDS):
    fields.extend(list(getattr(cls, fn, [])))

  state = dict()
  for n in fields:
    fv = getattr(obj, n, None)
    if fv is None and not n.startswith('_'):
      # Handle cases where fields are hidden.
      fv = getattr(obj, f'_{n}', None)

    state[n] = fv

  return state


def to_state(obj, path):
  state = get_state(obj)
  with open(path, mode='wb') as sfd:
    pickle.dump(state, sfd)


def from_state(cls, path, *args, **kwargs):
  with open(path, mode='rb') as sfd:
    state = pickle.load(sfd)

  skwargs = {n: state[n] for n in getattr(cls, KWARGS_FIELDS, [])}
  skwargs.update(kwargs)

  sargs = list(args) + [state[an] for an in getattr(cls, ARGS_FIELDS, [])]

  obj = cls(*sargs, **skwargs)

  for sn in getattr(cls, STATE_FIELDS, []):
    setattr(obj, sn, state[sn])

  finit = getattr(obj, '_state_finit', None)
  if finit is not None:
    finit()

  return obj

