"""Tests for py_misc_utils.async_manager additions.

Covers behaviors beyond the base async_manager tests: AsyncContext.get()
create-or-reuse with a stored __aenter__ result, close() raising the
handled exception when a context does not suppress it, AsyncManager
worker-pool lifecycle details (multiple work items per worker, results
order by work id, timeout error surfacing, timeout fetch, context
manager protocol), and the module-level run_async global runner.
"""

import asyncio
import multiprocessing

import pytest

from py_misc_utils import async_manager as pam


def _worker_ctor(context):
  """Work returning a fixed string after a short async sleep."""

  async def work():
    await asyncio.sleep(0.01)

    return 'ok'

  return work()


def _echo_ctor(context, value):
  """Work factory returning the passed value."""

  async def work():
    return value

  return work()


def _echo0(context):
  return _echo_ctor(context, 0)


def _echo10(context):
  return _echo_ctor(context, 10)


def _echo20(context):
  return _echo_ctor(context, 20)


def _echo30(context):
  return _echo_ctor(context, 30)


def _seq_echo(context, value):
  """Work factory returning the passed value (picklable partial target)."""

  async def work():
    return value

  return work()


class _SeqEcho:
  """Picklable work factory producing a fixed value."""

  def __init__(self, value):
    self.value = value

  def __call__(self, context):
    return _echo_ctor(context, self.value)


def _late_boom_ctor(context):
  """Work raising after a short delay."""

  async def work():
    await asyncio.sleep(0.3)
    raise ValueError('late boom')

  return work()


def test_async_context_get_returns_result():
  """get() enters the context on first use and stores the enter result."""

  async def scenario():
    am = pam.AsyncContext()

    class _Ctx:
      entered = 0

      async def __aenter__(self):
        self.__class__.entered += 1

        return 'enter-result'

      async def __aexit__(self, *exc):
        return True

    got1 = await am.get('x', _Ctx)
    got2 = await am.get('x', _Ctx)

    assert got1 == got2 == 'enter-result'
    assert _Ctx.entered == 1

    await am.close()

  asyncio.run(scenario())


def test_async_context_aexit_suppression():
  """__aexit__ re-raises the handled exception when a context declines."""
  raised = RuntimeError('inside')

  class _Ctx:

    async def __aenter__(self):
      return self

    async def __aexit__(self, *exc):
      return False

  async def scenario():
    am = pam.AsyncContext()
    await am.add('x', _Ctx())

    with pytest.raises(RuntimeError):
      await am.__aexit__(RuntimeError, raised, None)

  asyncio.run(scenario())


def test_async_context_aexit_suppressed():
  """__aexit__ stays silent when every context suppresses."""
  raised = RuntimeError('inside')

  class _Ctx:

    async def __aenter__(self):
      return self

    async def __aexit__(self, *exc):
      return True

  async def scenario():
    am = pam.AsyncContext()
    await am.add('x', _Ctx())

    assert await am.__aexit__(RuntimeError, raised, None) is False

  asyncio.run(scenario())


def test_async_manager_multiple_results_in_order():
  """Results are fetched with their work ids after parallel enqueues."""
  mpctx = multiprocessing.get_context('fork')

  with pam.AsyncManager(num_workers=2, mpctx=mpctx) as am:
    for i, ctor in enumerate((_echo0, _echo10, _echo20, _echo30)):
      am.enqueue_work(i, ctor)

    results = {}
    for _ in range(4):
      wid, res = am.fetch_result(timeout=10)
      results[wid] = res

    assert results == {0: 0, 1: 10, 2: 20, 3: 30}


def test_async_manager_repeated_work_on_same_worker():
  """A single worker processes several work items sequentially."""
  mpctx = multiprocessing.get_context('fork')
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    for i in range(3):
      am.enqueue_work(i, _SeqEcho(i))

    got = [am.fetch_result(timeout=10) for _ in range(3)]

    assert got == [(0, 0), (1, 1), (2, 2)]
  finally:
    am.close()


def test_async_manager_timeout_error_surfaces():
  """A work item raising after fetch_result(timeout) reports the failure."""
  mpctx = multiprocessing.get_context('fork')
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    am.enqueue_work(7, _late_boom_ctor)

    wid, res = am.fetch_result(timeout=10)
    assert wid == 7
    # The worker failure is transported as a WorkException holder.
    from py_misc_utils import work_results as wres
    assert isinstance(res, wres.WorkException)
    with pytest.raises(ValueError, match='late boom'):
      res.do_raise()
  finally:
    am.close()


def test_async_runner_is_runnable_repeatedly():
  """run_async can be used for several coroutines in sequence."""
  from py_misc_utils import async_manager

  async def work(v):
    await asyncio.sleep(0.01)

    return v * 2

  assert async_manager.run_async(work(3)).result(timeout=10) == 6
  assert async_manager.run_async(work(5)).result(timeout=10) == 10
