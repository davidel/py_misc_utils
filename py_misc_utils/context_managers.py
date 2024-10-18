
class CtxManager:

  def __init__(self, infn, outfn):
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    return self._infn()

  def __exit__(self, *exc):
    return self._outfn(*exc)


class CtxManagerWrapper:

  def __init__(self, wrap_ctx, wrap_obj=None):
    self._wrap_ctx = wrap_ctx
    self._wrap_obj = wrap_obj

  def __enter__(self):
    wres = self._wrap_ctx.__enter__()

    return wres if self._wrap_obj is None else self._wrap_obj

  def __exit__(self, *exc):
    return self._wrap_ctx.__exit__(*exc)


class NoOpCtxManager:

  def __init__(self, obj):
    self._obj = obj

  def __enter__(self):
    return self._obj

  def __exit__(self, *exc):
    return False
