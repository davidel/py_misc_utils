import inspect
import itertools
import pickle

from . import alog
from . import assert_checks as tas
from . import utils as ut


ARGS_FIELDS = 'ARGS_FIELDS'
KWARGS_FIELDS = 'KWARGS_FIELDS'
STATE_FIELDS = 'STATE_FIELDS'


def get_state(obj):
  cls = obj.__class__
  fields = []
  for fn in (ARGS_FIELDS, KWARGS_FIELDS, STATE_FIELDS):
    fields.extend(list(getattr(cls, fn, [])))

  state, missing = dict(), object()
  for n in fields:
    fv = getattr(obj, n, missing)
    if fv is missing and not n.startswith('_'):
      # Handle cases where fields are stored as hidden.
      fv = getattr(obj, f'_{n}', missing)

    if fv is missing:
      alog.xraise(RuntimeError, f'Missing field: "{n}" and "_{n}"')

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

  # Use islice() to skip the first parameter, which for an unbound __init__()
  # is going to be "self".
  sig, missing = inspect.signature(cls.__init__), object()
  for n, p in itertools.islice(sig.parameters.items(), 1, None):
    pv = kwargs.get(n, missing)
    if pv is not missing:
      skwargs[n] = pv

  sargs = list(args) + [state[an] for an in getattr(cls, ARGS_FIELDS, [])]

  obj = cls(*sargs, **skwargs)

  for sn in getattr(cls, STATE_FIELDS, []):
    setattr(obj, sn, state[sn])

  finit = getattr(obj, '_state_finit', None)
  if callable(finit):
    finit()

  return obj

