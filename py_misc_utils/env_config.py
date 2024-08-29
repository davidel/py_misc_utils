import argparse

from . import utils as ut


class EnvConfig:

  def __init__(self):
    parser = argparse.ArgumentParser()
    state = dict()
    for name in dir(self):
      if not name.startswith('_'):
        value = getattr(self, name)
        # Do not try to override functions (even though there really should not
        # be in an EnvConfig derived object).
        if not callable(value):
          env = ut.getenv(name, dtype=type(value))
          if env is not None:
            value = env

          parser.add_argument(f'--{name}', type=type(value))
          state[name] = value

    args, _ = parser.parse_known_args()
    for name, value in state.items():
      avalue = getattr(args, name, None)
      setattr(self, name, value if avalue is None else avalue)

