from . import utils as ut


class EnvConfig:

  def __init__(self):
    for name in dir(self):
      if name.isupper():
        value = getattr(self, name)
        env = ut.getenv(name, dtype=type(value))
        if env is not None:
          setattr(self, name, env)

