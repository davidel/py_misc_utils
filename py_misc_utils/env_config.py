from . import utils as ut


class EnvConfig:

  def __init__(self):
    for name in dir(self):
      if not name.startswith('_'):
        value = getattr(self, name)
        # Do not try to override functions (even though there really should not
        # be in an EnvConfig derived object).
        if not callable(value):
          env = ut.getenv(name, dtype=type(value))
          if env is not None:
            setattr(self, name, env)

