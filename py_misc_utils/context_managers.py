import contextlib


class CtxManager:

  def __init__(self, infn, outfn):
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    return self._infn()

  def __exit__(self, *exc):
    return self._outfn(*exc)


class Wrapper:

  def __init__(self, v, attr=None, finfn=None):
    self.v = v
    self._finfn = finfn or getattr(v, attr or 'close')

  def __enter__(self):
    return self

  def detach(self):
    self._finfn = None

    return self.v

  def __exit__(self, *exc):
    if self._finfn is not None:
      self._finfn()

    return False


class Pack(contextlib.ExitStack):

  def __init__(self, *wrap_ctxs, wrap_obj=None, wrap_idx=None):
    super().__init__()
    self._wrap_ctxs = wrap_ctxs
    self._wrap_obj = wrap_obj
    self._wrap_idx = wrap_idx

  def __enter__(self):
    super().__enter__()

    try:
      wres = [self.enter_context(ctx) for ctx in self._wrap_ctxs]
    except:
      self.close()
      raise

    if self._wrap_obj is not None:
      return self._wrap_obj

    return wres[-1] if self._wrap_idx is None else wres[self._wrap_idx]


def detach(obj):
  return obj.detach() if isinstance(obj, CtxManagerProxy) else obj


def cond(value, ctxfn, *args, **kwargs):
  return ctxfn(*args, **kwargs) if value else contextlib.nullcontext()

