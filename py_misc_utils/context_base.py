from . import utils as ut


class ContextBase:

  def __init__(self, default_ctx):
    self._ctx = ut.make_object(**default_ctx)

  def _new_ctx(self, **kwargs):
    ctx = self._ctx
    args = ctx.__dict__.copy()
    args.update(**kwargs)

    nctx = ut.make_object(**args)
    self._ctx = nctx

    return nctx

