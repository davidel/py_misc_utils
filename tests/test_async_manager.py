"""Tests for the py_misc_utils.async_manager async work machinery.

Expectations follow the documented API contract: AsyncContext enters and
exits all managed contexts on close and re-raises the handled exception
when a context does not suppress it, get() creates-or-returns named
contexts, AsyncRunner runs coroutines on a background event loop thread,
run_async() uses the process-global runner, and AsyncManager distributes
work over worker processes (fork context) returning (work_id, result)
tuples, with worker exceptions surfaced as WorkException payloads.
"""

import asyncio
import multiprocessing

import pytest

import py_misc_utils.async_manager as pam
import py_misc_utils.work_results as pwr


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

    # add() awaits __aenter__ and stores (context, aenter result).
    assert await am.add('a', ctx) is None
    assert am._contexts['a'][0] is ctx
    assert entered == ['enter']

    # get() on a known name returns the stored aenter result.
    assert await am.get('a', lambda: ctx) is None
    assert entered == ['enter']

    await am.remove('a')
    assert entered == ['enter', 'exit']
    assert 'a' not in am._contexts

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
    assert am._contexts == {}

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
  """AsyncRunner executes coroutines and shuts down its loop thread."""
  runner = pam.AsyncRunner()

  async def work(v):
    await asyncio.sleep(0.01)

    return v * 2

  fut = runner.run(work(21))

  assert fut.result(timeout=5) == 42

  runner.stop()

  assert not runner._thread.is_alive()


def test_run_async_uses_global_runner():
  """run_async() submits to the process-global runner."""

  async def work():
    return 'done'

  fut = pam.run_async(work())

  assert fut.result(timeout=5) == 'done'


def test_async_manager_distributes_and_fetches():
  """AsyncManager runs work items and returns (work_id, result) pairs."""
  mpctx = multiprocessing.get_context('fork')
  am = pam.AsyncManager(num_workers=2, mpctx=mpctx)

  try:
    for i in range(4):
      am.enqueue_work(f'w{i}', _worker_ctor)

    results = {}
    for _ in range(4):
      wid, res = am.fetch_result(timeout=10)
      results[wid] = res

    assert results == {f'w{i}': 'ok' for i in range(4)}
    # Every worker's queued-work counter is back to zero.
    assert not am._queued.any()
  finally:
    am.close()


def test_async_manager_surfaces_worker_errors():
  """Worker exceptions come back as WorkException payloads."""
  mpctx = multiprocessing.get_context('fork')
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    am.enqueue_work('boom', _failing_ctor)
    wid, res = am.fetch_result(timeout=10)

    assert wid == 'boom'
    assert isinstance(res, pwr.WorkException)
    assert res.is_instance(ValueError)
  finally:
    am.close()


def test_async_manager_fetch_nonblocking_empty():
  """fetch_result(block=False) on an idle manager returns None."""
  mpctx = multiprocessing.get_context('fork')
  am = pam.AsyncManager(num_workers=1, mpctx=mpctx)

  try:
    assert am.fetch_result(block=False) is None
  finally:
    am.close()
