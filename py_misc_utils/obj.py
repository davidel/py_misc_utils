import copy


class Obj:

  def __init__(self, **kwargs):
    self.update(**kwargs)

  def update(self, **kwargs):
    vars(self).update(kwargs)

    return self

  def update_from(self, obj):
    vars(self).update(vars(obj))

    return self

  def clone(self, **kwargs):
    nobj = copy.copy(self)
    nobj.update(**kwargs)

    return nobj

  def as_dict(self):
    ad = dict()
    for k, v in vars(self).items():
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

  def __eq__(self, other):
    missing = object()
    for k, v in vars(self).items():
      ov = getattr(other, k, missing)
      if ov is missing or v != ov:
        return False
    for k in vars(other).keys():
      if not hasattr(self, k):
        return False

    return True

  def __repr__(self):
    values = ', '.join(f'{k}={str_value(v)}' for k, v in vars(self).items())

    return f'{type(self).__name__}({values})'


def str_value(v):
  return '"' + v.replace('"', '\\"') + '"' if isinstance(v, str) else str(v)

