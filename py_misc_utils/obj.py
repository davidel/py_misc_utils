

class Obj:

  def __init__(self, **kwargs):
    self.update(**kwargs)

  def update(self, **kwargs):
    self.__dict__.update(kwargs)

  def as_dict(self):
    ad = dict()
    for k, v in self.__dict__.items():
      if isinstance(v, Obj):
        v = v.as_dict()
      elif isinstance(v, (list, tuple)):
        vals = []
        for x in v:
          if isinstance(x, Obj):
            x = x.as_dict()
          vals.append(x)

        v = type(v)(vals)
      elif isinstance(v, dict):
        vd = dict()
        for z, x in v.items():
          if isinstance(x, Obj):
            x = x.as_dict()
          vd[z] = x

        v = vd

      ad[k] = v

    return ad

  def __repr__(self):
    vstr = '"' + v.replace('"', '\\"') + '"' if isinstance(v, str) else str(v)
    values = ', '.join(f'{k}={vstr}' for k, v in self.__dict__.items())

    return f'{type(self).__name__}({values})'

