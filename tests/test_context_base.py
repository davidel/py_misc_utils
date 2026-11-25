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



