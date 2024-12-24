import weakref

from . import assert_checks as tas


def _finalizer_name(name):
  return f'__{name}_finalizer'


class fin_wrap:

  def __init__(self, parent, name, obj, finfn=None, cleanup=False):
    setattr(parent, name, obj)
    fname = _finalizer_name(name)
    if obj is not None:
      tas.check_is_not_none(finfn, msg=f'Missing finalize function argument')

      setattr(parent, fname, self)
      self._finalizer = weakref.finalize(self, finfn)
    else:
      fwrap = getattr(parent, fname, None)
      if fwrap is not None:
        delattr(parent, fname)
        if cleanup:
          fwrap._finalizer()
        else:
          fwrap._finalizer.detach()


class _Wrapper:
  pass

def fin_wrap_np(obj, finfn, name='v'):
  wrapper = _Wrapper()
  fin_wrap(wrapper, name, obj, finfn=finfn)

  return wrapper


_OBJ_NAME = 'wrapped_obj'
_RESERVED_NAMES = {_OBJ_NAME, _finalizer_name(_OBJ_NAME)}

class FinWrapper:

  def __init__(self, obj, finfn):
    fin_wrap(self, _OBJ_NAME, obj, finfn=finfn)

  def __getattribute__(self, name):
    pd = super().__getattribute__('__dict__')
    obj = pd[_OBJ_NAME]

    return getattr(obj, name)

  def __getattr__(self, name):
    pd = super().__getattribute__('__dict__')
    obj = pd[_OBJ_NAME]

    return getattr(obj, name)

  def __setattr__(self, name, value):
    pd = super().__getattribute__('__dict__')
    if name in _RESERVED_NAMES:
      pd[name] = value
    else:
      obj = pd[_OBJ_NAME]

      setattr(obj, name, value)

  def __delattr__(self, name):
    pd = super().__getattribute__('__dict__')
    if name in _RESERVED_NAMES:
      pd.pop(name)
    else:
      obj = pd[_OBJ_NAME]

      delattr(obj, name)

