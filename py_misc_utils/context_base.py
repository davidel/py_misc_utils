"""Base class for objects holding a context-like set of options.

This module provides ContextBase, a small base class holding a context
object - a namespace of option values - which can be cloned with
overrides: subclasses use it to implement APIs where a set of options
can be customized per-call without altering the shared defaults.
"""
from . import utils as ut


class ContextBase:
  """A base class holding an overridable context of options.

  The context is stored as a plain object - built via
  utils.make_object() - whose attributes are the option values.
  """

  def __init__(self, default_ctx):
    """Creates a context holder.

    Args:
      default_ctx: A dict of the default option values.
    """
    self._ctx = ut.make_object(**default_ctx)

  def _new_ctx(self, **kwargs):
    """Creates a new context, overriding the current option values.

    The new context becomes the current one - i.e. subsequent _new_ctx()
    calls clone it in turn.

    Args:
      **kwargs: The option overrides.

    Returns:
      The newly created context object.
    """
    ctx = self._ctx
    args = ctx.__dict__.copy()
    args.update(**kwargs)

    nctx = ut.make_object(**args)
    self._ctx = nctx

    return nctx
