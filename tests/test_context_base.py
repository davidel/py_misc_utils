"""Tests for py_misc_utils.context_base.ContextBase.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): ContextBase takes a default_ctx dict of default option
values; _new_ctx() clones the current context with overrides and makes
the clone the current context - so subsequent _new_ctx() calls chain the
overrides while previously returned contexts stay untouched.
"""

from py_misc_utils import context_base as cb


class Opt(cb.ContextBase):
  pass


def test_initial_context():
  """default_ctx dict entries become attributes of the context."""
  opt = Opt(dict(alpha=1, beta='b'))
  assert opt._ctx.alpha == 1
  assert opt._ctx.beta == 'b'


def test_default_context_empty():
  """With an empty default_ctx the context holds no options."""
  opt = Opt(dict())
  assert len(opt._ctx.__dict__) == 0


def test_new_ctx_overrides():
  """_new_ctx returns the new context with overridden values."""
  opt = Opt(dict(alpha=1, beta='b'))

  nctx = opt._new_ctx(alpha=10)

  assert nctx is opt._ctx
  assert nctx.alpha == 10
  assert nctx.beta == 'b'


def test_new_ctx_chains():
  """A later _new_ctx clones the previous one, keeping prior overrides."""
  opt = Opt(dict(alpha=1, beta='b'))

  opt._new_ctx(alpha=10)
  opt._new_ctx(gamma=True)

  assert opt._ctx.alpha == 10
  assert opt._ctx.beta == 'b'
  assert opt._ctx.gamma is True


def test_new_ctx_leaves_previous():
  """The previous context object is untouched by a later override."""
  opt = Opt(dict(alpha=1))

  first = opt._ctx
  second = opt._new_ctx(alpha=2)

  assert first is not second
  assert first.alpha == 1
