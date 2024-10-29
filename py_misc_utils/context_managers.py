import contextlib


class CtxManager:

  def __init__(self, infn, outfn):
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    return self._infn()

  def __exit__(self, *exc):
    return self._outfn(*exc)


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

