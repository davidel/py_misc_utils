import weakref


class fin_wrap:

  def __init__(self, parent, name, obj, finfn=None):
    setattr(parent, name, obj)
    fname = f'__{name}_finalizer'
    if obj is not None:
      setattr(parent, fname, self)
      weakref.finalize(self, finfn or (lambda: obj.fin()))
    else:
      if hasattr(parent, fname):
        delattr(parent, fname)

