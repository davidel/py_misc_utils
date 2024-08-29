from . import utils as ut


class EnvConfig:

  def __init__(self):
    for name in dir(self):
      if not name.startswith('_'):
        value = getattr(self, name)
        if not callable(value):
          env = ut.getenv(name, dtype=type(value))
          if env is not None:
            setattr(self, name, env)

