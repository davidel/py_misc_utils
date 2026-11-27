"""Context manager helpers.

This module provides a set of small context-manager utilities: adapters
turning in/out function pairs into context managers, wrappers closing the
wrapped object upon exit, a pack of context managers entered together,
and helpers to conditionally-activate a context manager.

Example:
  import os
  from py_misc_utils import context_managers as cm

  pack = cm.Pack(cm.Wrapper(open(os.devnull, 'w')))
  with pack as w:
    assert w.v.closed is False

  assert w.v.closed is True
"""
import contextlib


class CtxManager:
  """A context manager built from an enter and an exit function.

  Example:
    from py_misc_utils import context_managers as cm

    log = []

    def outfn(*exc):
      log.append('out')

      return False

    with cm.CtxManager(lambda: log.append('in'), outfn):
      assert log == ['in']

    assert log == ['in', 'out']
  """

  def __init__(self, infn, outfn):
    """Creates a context manager adapter.

    Args:
      infn: The function called upon entering the context, whose result
        is returned by __enter__().
      outfn: The function called upon exiting the context - passed the
        __exit__() exception info, if any.
    """
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    """Enters the context, returning the infn() result."""

    return self._infn()

  def __exit__(self, *exc):
    """Exits the context, delegating to the outfn() function."""

    return self._outfn(*exc)


class Wrapper:
  """A context manager wrapping an object with a close operation.

  Entering the wrapper returns the wrapper itself, so that the wrapped
  object can be accessed - and possibly detached - via the 'v'
  attribute. Upon exit the close operation is executed, unless the
  wrapper has been detached.
  """

  def __init__(self, v, attr=None, finfn=None):
    """Creates an object wrapper.

    Args:
      v: The wrapped object.
      attr: An optional attribute name of the wrapped object holding the
        close operation - 'close' by default. Ignored if finfn is given.
      finfn: An optional close operation to execute upon exiting the
        context. If None, the wrapped object close operation is used.
    """
    self.v = v
    self._finfn = finfn or getattr(v, attr or 'close')

  def __enter__(self):
    """Enters the context, returning the wrapper itself."""

    return self

  def detach(self):
    """Detaches the wrapped object, preventing its close upon exit.

    Returns:
      The wrapped object.
    """
    self._finfn = None

    return self.v

  def __exit__(self, *exc):
    """Exits the context, closing the wrapped object - if not detached."""

    if self._finfn is not None:
      self._finfn()

    return False


class Pack(contextlib.ExitStack):
  """A context manager entering a set of context managers together.

  All the context managers are entered upon entering the Pack, and their
  results can be retrieved - either by index, or by providing a custom
  wrapping object. If any of the context managers fails to be entered,
  all the ones already entered are exited back.
  """

  def __init__(self, *wrap_ctxs, wrap_obj=None, wrap_idx=None):
    """Creates a context managers pack.

    Args:
      *wrap_ctxs: The context managers to be entered together.
      wrap_obj: An optional object to be returned by __enter__(), instead
        of the entered context managers results.
      wrap_idx: An optional index of the entered context manager whose
        result is returned by __enter__() - the last one by default.
    """
    super().__init__()
    self._wrap_ctxs = wrap_ctxs
    self._wrap_obj = wrap_obj
    self._wrap_idx = wrap_idx

  def __enter__(self):
    """Enters all the context managers of the pack.

    Returns:
      The wrap_obj - if given - or the result of the entered context
      manager selected by wrap_idx.
    """
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
  """Detaches an object from its context manager wrapper - if wrapped.

  Wrapper instances get their close operation cancelled, so that the
  wrapped object is not closed when the wrapping context exits; any
  other object is returned unchanged.

  Args:
    obj: The object to detach.

  Returns:
    The detached object.
  """
  return obj.detach() if isinstance(obj, Wrapper) else obj


def cond(value, ctxfn, *args, **kwargs):
  """Returns a context manager active only if a condition holds.

  Args:
    value: The activation condition.
    ctxfn: The context manager builder function - called only if the
      condition holds.
    *args: Positional arguments passed to the context manager builder.
    **kwargs: Keyword arguments passed to the context manager builder.

  Returns:
    The built context manager if the condition holds, or a null context
    manager otherwise.
  """
  return ctxfn(*args, **kwargs) if value else contextlib.nullcontext()
