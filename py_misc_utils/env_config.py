from . import utils as ut


_OVERRIDE_TYPES = {int, float, str, bool}

class EnvConfig:

  def __init__(self):
    for name in dir(self):
      if not name.startswith('_'):
        value = getattr(self, name)
        if type(value) in _OVERRIDE_TYPES:
          env = ut.getenv(name, dtype=type(value))
          if env is not None:
            setattr(self, name, env)

