import weakref

from . import assert_checks as tas


class fin_wrap:

  def __init__(self, parent, name, obj, finfn=None):
    setattr(parent, name, obj)
    fname = f'__{name}_finalizer'
    if obj is not None:
      tas.check_is_not_none(finfn, msg=f'Missing finalize function argument')

      setattr(parent, fname, self)
      weakref.finalize(self, finfn)
    else:
      if hasattr(parent, fname):
        delattr(parent, fname)

