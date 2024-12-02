import contextlib


class CtxManager:

  def __init__(self, infn, outfn):
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    return self._infn()

  def __exit__(self, *exc):
    return self._outfn(*exc)


class CtxManagerProxy:

  def __init__(self, obj):
    self._obj = obj
    self.v = None

  def __enter__(self):
    self.v = self._obj.__enter__()

    return self

  def detach(self):
    v = self.v
    self._obj = None
    self.v = None

    return v

  def __exit__(self, *exc):
    return self._obj.__exit__(*exc) if self._obj is not None else False


class CtxManagerWrapper(contextlib.ExitStack):

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

