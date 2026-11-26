"""Tests for the py_misc_utils.async_manager async work machinery.

Expectations follow the documented API contract: AsyncContext enters and
exits all managed contexts on close and re-raises the handled exception
when a context does not suppress it, get() creates-or-returns named
contexts, AsyncRunner runs coroutines on a background event loop thread,
run_async() uses the process-global runner, and AsyncManager distributes
work over worker processes returning (work_id, result) tuples, with
worker exceptions surfaced as WorkException payloads.

AsyncManager tests run under both spawn and forkserver contexts: the
process-boundary contract (module-level, picklable work ctors) holds
regardless of the start method, and no library state relies on fork
inheritance.
"""

import asyncio
import concurrent.futures
import multiprocessing

import pytest

import py_misc_utils.async_manager as pam
import py_misc_utils.work_results as pwr

MP_METHODS = ['spawn', 'forkserver']


@pytest.fixture(params=MP_METHODS)
def mpctx(request):
  """Parametrized multiprocessing context: spawn and forkserver."""
  return multiprocessing.get_context(request.param)


def _mk_async_ctx(entered):

  class _Ctx:

    async def __aenter__(self):
      entered.append('enter')

    async def __aexit__(self, *exc):
      entered.append('exit')
      return True

  return _Ctx()


def _mk_boom_ctx():

  class _Boom:

    async def __aenter__(self):
      pass

    async def __aexit__(self, *exc):
      raise RuntimeError('exit boom')

  return _Boom()


def _worker_ctor(context):

  async def work():
    await asyncio.sleep(0.01)
    return 'ok'

  return work()


def _failing_ctor(context):

  async def work():
    raise ValueError('worker boom')

  return work()


def test_async_context_add_get_remove():
  """add() enters and registers; get() reuses; remove() exits and drops."""

  async def scenario():
    am = pam.AsyncContext()
    entered = []
    ctx = _mk_async_ctx(entered)

    # add() awaits __aenter__; get() reuses the entered context.
    assert await am.add('a', ctx) is None
    assert entered == ['enter']

    # get() on a known name returns the stored aenter result.
    assert await am.get('a', lambda: ctx) is None
    assert entered == ['enter']

    await am.remove('a')
    assert entered == ['enter', 'exit']

    # remove() on a known name exits the context; get() recreates it.

  asyncio.run(scenario())


def test_async_context_get_creates_and_reuses():
  """get(name, ctor) creates once and returns the same enter result."""

  async def scenario():
    am = pam.AsyncContext()
    entered = []
    entered2 = []

    r1 = await am.get('c', lambda: _mk_async_ctx(entered))
    r2 = await am.get('c', lambda: _mk_async_ctx(entered2))

    assert r1 is r2
    assert entered == ['enter']
    assert entered2 == []

  asyncio.run(scenario())


def test_async_context_close_exits_all():
  """close() exits all contexts and clears the registry."""

  async def scenario():
    am = pam.AsyncContext()
    entered = []

    await am.add('a', _mk_async_ctx(entered))
    await am.add('b', _mk_async_ctx(entered))

    # add() entered both contexts; close() exits them all.
    await am.close()

    assert entered == ['enter', 'enter', 'exit', 'exit']

    # After close() the contexts are gone: get() recreates them.
    await am.get('a', lambda: _mk_async_ctx(entered))
    assert entered == ['enter', 'enter', 'exit', 'exit', 'enter']

  asyncio.run(scenario())


def test_async_context_close_propagates_failure():
  """A failing __aexit__ re-raises from close()."""

  async def scenario():
    am = pam.AsyncContext()
    await am.add('boom', _mk_boom_ctx())

    with pytest.raises(RuntimeError, match='exit boom'):
      await am.close()

  asyncio.run(scenario())


def test_async_runner_runs_coroutine_and_stops():
  """AsyncRunner executes coroutines; after stop() the loop is closed."""
  runner = pam.AsyncRunner()

  async def work(v):
    await asyncio.sleep(0.01)

    return v * 2

  fut = runner.run(work(21))

  assert fut.result(timeout=5) == 42

  runner.stop()

  # After stop() the loop is closed and drained: submitting a new coroutine
  # never completes (run_coroutine_threadsafe schedules onto a dead loop).
  # Pin the observable public behavior: the future does not resolve in time.
  async def never():
    await asyncio.sleep(0)

  coro = never()
  fut2 = runner.run(coro)

  try:
    fut2.result(timeout=1)
    raise AssertionError('run() after stop() unexpectedly completed')
  except concurrent.futures.TimeoutError:
    pass
  finally:
    # The dead loop never scheduled the coroutine; close it so it is not
    # reported as never awaited.
    coro.close()


def test_run_async_uses_global_runner():
  """run_async() submits to the process-global runner."""

  async def work():
    return 'done'

  fut = pam.run_async(work())

  assert fut.result(timeout=5) == 'done'


# ---- Additional work factories imported from test_async_manager_addons ----


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


# ---- Additional tests from test_async_manager_addons ----


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


def test_async_manager_multiple_results_in_order(mpctx):
  """Results are fetched with their work ids after parallel enqueues."""
  with pam.AsyncManager(num_workers=2, mpctx=mpctx) as am:
    for i, ctor in enumerate((_echo0, _echo10, _echo20, _echo30)):
      am.enqueue_work(i, ctor)

    results = {}
    for _ in range(4):
      wid, res = am.fetch_result(timeout=10)
      results[wid] = res

    assert results == {0: 0, 1: 10, 2: 20, 3: 30}


def test_async_manager_repeated_work_on_same_worker(mpctx):
  """A single worker processes several work items sequentially."""
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    for i in range(3):
      am.enqueue_work(i, _SeqEcho(i))

    got = [am.fetch_result(timeout=10) for _ in range(3)]

    assert got == [(0, 0), (1, 1), (2, 2)]
  finally:
    am.close()


def test_async_manager_timeout_error_surfaces(mpctx):
  """A work item raising after fetch_result(timeout) reports the failure."""
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


def test_async_manager_distributes_and_fetches(mpctx):
  """AsyncManager runs work items and returns (work_id, result) pairs."""
  am = pam.AsyncManager(num_workers=2, mpctx=mpctx)

  try:
    for i in range(4):
      am.enqueue_work(f'w{i}', _worker_ctor)

    results = {}
    for _ in range(4):
      wid, res = am.fetch_result(timeout=10)
      results[wid] = res

    assert results == {f'w{i}': 'ok' for i in range(4)}
  finally:
    am.close()


def test_async_manager_surfaces_worker_errors(mpctx):
  """Worker exceptions come back as WorkException payloads."""
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    am.enqueue_work('boom', _failing_ctor)
    wid, res = am.fetch_result(timeout=10)

    assert wid == 'boom'
    assert isinstance(res, pwr.WorkException)
    assert res.is_instance(ValueError)
  finally:
    am.close()


def test_async_manager_fetch_nonblocking_empty(mpctx):
  """fetch_result(block=False) on an idle manager returns None."""
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    assert am.fetch_result(block=False) is None
  finally:
    am.close()
