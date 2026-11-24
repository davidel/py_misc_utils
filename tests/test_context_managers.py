"""Tests for py_misc_utils.context_managers.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): CtxManager adapts an in/out function pair into a
context manager passing the exit exception info to outfn; Wrapper wraps
an object closing it on exit unless detached - __enter__ returns the
wrapper itself, the object is reachable via .v; Pack (an ExitStack)
enters a set of managers and returns the LAST entered Wrapper (or the
one selected by wrap_idx); cm.detach() is report-only broken (undefined
CtxManagerProxy name); cond() activates a context manager only when the
condition holds (nullcontext otherwise).
"""

import contextlib

import pytest

from py_misc_utils import context_managers as cm


class Resource:

  def __init__(self, log):
    self.log = log

  def open(self):
    self.log.append('open')

    return self

  def close(self):
    self.log.append('close')


def test_ctx_manager_adapter():
  """CtxManager pairs an enter and an exit function.

  outfn is invoked with the __exit__ exception info (3 arguments even
  on clean exit), so adapters take *exc - a plain close() is not
  directly usable as outfn (report-only API sharp edge).
  """
  log = []
  res = Resource(log)

  def outfn(*exc):
    log.append('close')

  with cm.CtxManager(res.open, outfn) as r:
    assert r is res

  assert log == ['open', 'close']


def test_ctx_manager_outfn_gets_exc_info():
  """outfn receives the 3-element exception info tuple on error."""
  got = []

  def outfn(*exc):
    got.append(exc)

    return False

  with pytest.raises(ValueError), cm.CtxManager(lambda: 1, outfn):
    raise ValueError('boom')

  assert len(got) == 1
  assert isinstance(got[0][1], ValueError)


def test_wrapper_closes_on_exit():
  """Wrapper closes the wrapped object at exit; __enter__ yields it."""
  log = []
  res = Resource(log)

  w = cm.Wrapper(res.open(), 'close')
  with w as r:
    assert r is w
    assert r.v is res

  assert log == ['open', 'close']


def test_wrapper_default_close_attr():
  """The attr argument defaults to 'close'."""
  log = []
  res = Resource(log)

  with cm.Wrapper(res.open()):
    pass

  assert log == ['open', 'close']


def test_wrapper_custom_finfn():
  """A custom finfn overrides the attribute-based close operation."""
  log = []
  res = Resource(log)

  with cm.Wrapper(res, finfn=lambda: log.append('custom')):
    pass

  assert log == ['custom']


def test_wrapper_detach():
  """detach() prevents the close from happening at exit."""
  log = []
  res = Resource(log)

  w = cm.Wrapper(res.open(), 'close')
  assert w.detach() is res

  with w:
    pass

  assert log == ['open']


def test_pack_returns_last_wrapper():
  """Pack yields the last entered Wrapper; objects close at exit."""
  log = []
  res1 = Resource(log)
  res2 = Resource(log)

  with cm.Pack(cm.Wrapper(res1), cm.Wrapper(res2)) as pack:
    assert pack.v is res2
    assert log == []

  assert log == ['close', 'close']


def test_pack_by_wrap_idx():
  """wrap_idx selects which entered Wrapper is returned by __enter__."""
  log = []
  res1 = Resource(log)
  res2 = Resource(log)

  with cm.Pack(cm.Wrapper(res1), cm.Wrapper(res2), wrap_idx=0) as pack:
    assert pack.v is res1


def test_pack_with_wrap_obj():
  """wrap_obj replaces the returned value entirely."""
  log = []
  marker = object()

  with cm.Pack(cm.Wrapper(Resource(log)), wrap_obj=marker) as pack:
    assert pack is marker


def test_pack_rollbacks_on_error():
  """If entering a later manager fails, entered ones are exited back."""
  log = []

  class BadCtx:

    def __enter__(self):
      raise RuntimeError('nope')

    def __exit__(self, *exc):
      return False

  with pytest.raises(RuntimeError), cm.Pack(cm.Wrapper(Resource(log)), BadCtx()):
    pass

  assert log == ['close']


def test_pack_with_ctx_manager_adapter():
  """Pack works with CtxManager adapters as members."""
  log = []
  res = Resource(log)

  def outfn(*exc):
    log.append('close')

  with cm.Pack(cm.CtxManager(res.open, outfn)) as pack:
    assert pack is res

  assert log == ['open', 'close']


def test_detach_proxy_raises_nameerror():
  """Report-only bug: cm.detach() references an undefined
  CtxManagerProxy symbol, raising NameError for every input."""
  with pytest.raises(NameError):
    cm.detach(object())


def test_cond_true():
  """cond(True, ...) enters the built context manager.

  Note: the context manager arguments are evaluated eagerly - the
  resource is opened even before the condition is checked.
  """
  log = []
  res = Resource(log)

  with cm.cond(True, cm.Wrapper, res.open(), 'close') as r:
    assert r.v is res

  assert log == ['open', 'close']


def test_cond_false():
  """cond(False, ...) yields a nullcontext - entering returns None.

  Report-only sharp edge: the ctxfn arguments are evaluated eagerly, so
  the resource is opened (but never closed) even when cond() picks the
  nullcontext branch.
  """
  log = []
  res = Resource(log)

  with cm.cond(False, cm.Wrapper, res.open(), 'close') as r:
    assert r is None

  assert log == ['open']


def test_cond_false_ctxfn_not_called():
  """cond(False) never invokes the context builder function."""
  called = []

  def ctxfn():
    called.append(1)

    return contextlib.nullcontext()

  with cm.cond(False, ctxfn):
    pass

  assert called == []
