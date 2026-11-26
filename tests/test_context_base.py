"""Tests for py_misc_utils.context_base.ContextBase.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): ContextBase takes a default_ctx dict of default option
values; _new_ctx() clones the current context with overrides and makes
the clone the current context - so subsequent _new_ctx() calls chain the
overrides while previously returned contexts stay untouched. The clone
is shallow: nested mutable values are shared between contexts.
"""

from py_misc_utils import context_base as cb


class Opt(cb.ContextBase):

  def values(self):
    """Returns the current context values as a plain dict."""
    return self._ctx.__dict__.copy()

  def set(self, **kwargs):
    """Applies option overrides via _new_ctx()."""
    return self._new_ctx(**kwargs)


def test_initial_defaults():
  """Default ctx values become the initial context attributes."""
  o = Opt(dict(a=1, b='x'))

  assert o.values() == dict(a=1, b='x')


def test_initial_defaults_empty():
  """An empty default dict yields an empty context."""
  o = Opt(dict())

  assert o.values() == dict()


def test_new_ctx_applies_overrides():
  """Overrides replace values while other attributes carry over."""
  o = Opt(dict(a=1, b='x'))
  c1 = o.set(a=2)

  assert c1.a == 2
  assert c1.b == 'x'


def test_new_ctx_becomes_current():
  """The clone becomes the current context, so overrides chain."""
  o = Opt(dict(a=1, b='x'))
  o.set(a=2)
  c2 = o.set(b='y')

  assert c2.a == 2
  assert c2.b == 'y'


def test_previous_contexts_untouched():
  """Returned contexts are immutable snapshots of their time."""
  o = Opt(dict(a=1, b='x'))
  c1 = o.set(a=2)
  o.set(b='y')

  assert c1.a == 2
  assert c1.b == 'x'


def test_no_kwargs_clones_current():
  """A call without overrides clones the current context."""
  o = Opt(dict(a=1))
  o.set(a=2)
  c = o.set()

  assert c.a == 2


def test_new_attributes_added():
  """Overrides for unknown options are added as new attributes."""
  o = Opt(dict(a=1))
  c = o.set(brand_new='hello')

  assert c.brand_new == 'hello'


def test_shallow_copy_semantics():
  """Nested mutable values are shared between the clone and its source."""
  shared = dict(k=1)
  o = Opt(dict(nested=shared))
  c = o.set()

  c.nested['k'] = 42

  assert shared['k'] == 42


def test_instances_are_independent():
  """Context state is per instance, not shared at class level."""
  o1 = Opt(dict(v=1))
  o2 = Opt(dict(v=1))
  o1.set(v=2)

  assert o2.values() == dict(v=1)
