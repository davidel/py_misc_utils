"""Tests for py_misc_utils.call_limiter.

trigger()/limit_call() rate-limit per call site: the key is the
(filename, line number) of the first frame outside the target file,
i.e. the direct caller's location. With the default _filename (the
call_limiter module itself), the key is the trigger()/limit_call()
call site itself; with an explicit _filename, frames of that file are
skipped and the first frame outside it keys the count. Call sites
whose file never appears in the stack are unlimited (trigger returns
True).

Callers carrying a controlled filename are built with compile() and
types.FunctionType (no exec), so the explicit-filename path can be
exercised with a real frame chain.
"""

import types

from py_misc_utils import call_limiter as cl


def _make_caller(filename, body_lines, glb):
  """Build a callable whose code object carries the given filename.

  The body runs with the created function's filename in the call
  stack, as if the calls happened inside a module with that name.
  """
  src = 'def caller():\n' + ''.join(f'  {line}\n' for line in body_lines)
  code = compile(src, filename, 'exec')
  fn_code = next(const for const in code.co_consts
                 if isinstance(const, types.CodeType) and const.co_name == 'caller')

  return types.FunctionType(fn_code, glb, 'caller')


def test_default_filename_keys_the_call_site():
  # The default _filename keys each trigger()/limit_call() source line
  # independently: three distinct call sites with count=1 all fire.
  def test_body():
    a = cl.limit_call(1, lambda: 'a')
    b = cl.limit_call(1, lambda: 'b')
    c = cl.limit_call(1, lambda: 'c')

    return a, b, c

  assert test_body() == ('a', 'b', 'c')


def test_same_call_site_is_limited():
  # Four triggers from the same source line share one call site:
  # with count=2 only the first two go through.
  hits = []

  def test_body():
    for _ in range(4):
      if cl.trigger(__file__, 2):
        hits.append('t')

  test_body()

  assert hits == ['t', 't']


def test_explicit_filename_skips_target_frames(tmp_path):
  """Frames of the target file are skipped; the caller keys the count."""
  lib = str(tmp_path / 'lib.py')

  hits = []
  glb = {'trigger': cl.trigger, 'lib': lib, 'hits': hits}
  # Both trigger calls happen "inside" lib, at different source lines
  # of lib: their keys are the (test file, caller line) pairs.
  caller = _make_caller(lib, [
      "if trigger(lib, 1): hits.append('x')",
      "if trigger(lib, 1): hits.append('x')",
  ], glb)

  for _ in range(3):
    caller()

  # All six trigger calls resolve to the single frame outside lib: the
  # caller() call line in this test. One call site, count=1: only the
  # first fires, and the target-file frames never count.
  assert hits == ['x']


def test_explicit_filename_limited_call_sites(tmp_path):
  """Call sites inside the caller are limited independently."""
  lib = str(tmp_path / 'lib.py')

  hits = []
  glb = {'trigger': cl.trigger, 'lib': lib, 'hits': hits}
  # The same source line reached repeatedly: one call site, count=2.
  caller = _make_caller(lib, [
      "for _ in range(4):",
      "    if trigger(lib, 2): hits.append('t')",
  ], glb)

  caller()

  assert hits == ['t', 't']


def test_absent_filename_is_unlimited():
  # A filename that never appears in the stack leaves no key: trigger
  # returns True on every call.
  for _ in range(3):
    assert cl.trigger('/no/such/file/anywhere.py', 1) is True


def test_limit_call_returns_fn_result_within_limit():
  assert cl.limit_call(3, lambda: 'value') == 'value'


def test_limit_call_passes_arguments():
  seen = []

  def fn(*args, **kwargs):
    seen.append((args, kwargs))

    return 'r'

  res = cl.limit_call(1, fn, 1, x=2)

  assert res == 'r'
  assert seen == [((1,), {'x': 2})]


def test_limit_call_blocks_beyond_limit():
  # A single call site (one source line) called until the limit is
  # hit: the result is the function value while allowed, then None.
  counter = []

  def fn():
    counter.append(1)

    return f'call-{len(counter)}'

  results = [cl.limit_call(1, fn) for _ in range(3)]

  assert results == ['call-1', None, None]
  assert len(counter) == 1
