import weakref


class fin_wrap:

  def __init__(self, parent, name, obj, finfn=None):
    setattr(parent, name, obj)
    setattr(parent, f'__{name}_finalizer', self)
    weakref.finalize(self, finfn or (lambda: obj.fin()))

