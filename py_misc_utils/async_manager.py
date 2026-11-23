"""Async task management with multiprocessing workers and asyncio event loops.

This module provides:
- AsyncContext: Manages multiple async context managers
- AsyncManager: Pool of worker processes running asyncio event loops
- AsyncRunner: Single-threaded asyncio event loop in a background thread
- run_async(): Convenience function to submit coroutines to the global runner
"""

import asyncio
import collections
import multiprocessing
import os
import queue
import threading

import numpy as np

from . import cleanups
from . import global_namespace as gns
from . import multiprocessing as mp
from . import work_results as wres


class AsyncContext:
  """Manages multiple async context managers with shared lifecycle.

  Allows adding, removing, and getting context managers by name.
  Automatically enters/exits all contexts on close.
  """

  def __init__(self):
    """Initialize an empty context manager collection."""
    self._contexts = dict()

  async def add(self, name, context):
    """Add and enter a context manager.

    Args:
      name: Unique name for the context.
      context: Async context manager instance.

    Returns:
      Result of context.__aenter__().
    """
    result = await context.__aenter__()
    self._contexts[name] = (context, result)

    return result

  async def remove(self, name):
    """Remove and exit a context manager by name.

    Args:
      name: Name of the context to remove.
    """
    context, _ = self._contexts.pop(name)
    await context.__aexit__(None, None, None)

  async def get(self, name, context_ctor):
    """Get or create a context manager by name.

    Args:
      name: Name of the context.
      context_ctor: Callable returning a new async context manager instance.

    Returns:
      Result of context.__aenter__() for the context.
    """
    context_result = self._contexts.get(name)
    if context_result is None:
      result = await self.add(name, context_ctor())
    else:
      result = context_result[1]

    return result

  async def close(self, *exc):
    """Exit all managed contexts.

    Args:
      *exc: Exception info to pass to __aexit__ (type, value, traceback).

    Raises:
      Exception: If any context's __aexit__ returns False and an exception
        was being handled.
    """
    rexc = exc or (None, None, None)
    needs_raise = False
    for context, _ in self._contexts.values():
      exitres = await context.__aexit__(*rexc)
      needs_raise = needs_raise or not exitres

    self._contexts = dict()
    if needs_raise and rexc[1] is not None:
      raise rexc[1]

  async def __aenter__(self):
    """Enter the async context (returns self)."""
    return self

  async def __aexit__(self, *exc):
    """Exit all managed contexts."""
    await self.close(*exc)

    return False


Work = collections.namedtuple('Work', 'id, ctor')


class _Worker:
  """Internal worker process running an asyncio event loop.

  Each worker has its own process, event loop, and thread for feeding work
  from a multiprocessing queue into the event loop.
  """

  def __init__(self, mpctx, wid, out_queue):
    """Initialize and start the worker process.

    Args:
      mpctx: Multiprocessing context (e.g., multiprocessing or fork context).
      wid: Worker ID (index).
      out_queue: Multiprocessing queue for returning results.
    """
    self._wid = wid
    self._out_queue = out_queue
    self._in_queue = mpctx.Queue()
    self._proc = mp.create_process(self._run, context=mpctx)
    self._proc.start()

  def _run(self):
    """Main process entry point: create event loop and start feeder thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    thread = threading.Thread(target=self._work_feeder, args=(loop,), daemon=True)
    thread.start()

    loop.run_forever()
    thread.join()

  async def _task_runner(self, context, work):
    """Execute a work item within the shared context.

    Args:
      context: AsyncContext instance shared across work items.
      work: Work namedtuple with id and ctor (callable returning awaitable).
    """
    try:
      task = work.ctor(context=context)

      result = await task
    except Exception as ex:
      result = wres.WorkException(ex, workid=work.id)

    self._out_queue.put((self._wid, work.id, result))

  def _work_feeder(self, loop):
    """Thread function: pull work from queue and schedule on event loop.

    Args:
      loop: asyncio event loop to schedule coroutines on.
    """
    context = AsyncContext()

    while True:
      work = self._in_queue.get()
      if work is None:
        break

      asyncio.run_coroutine_threadsafe(self._task_runner(context, work), loop)

    asyncio.run_coroutine_threadsafe(self._shutdown(context, loop), loop)

  @classmethod
  async def _shutdown(cls, context, loop):
    """Shutdown coroutine: close context and stop event loop."""
    await context.close()
    loop.stop()

  def stop(self):
    """Stop the worker process."""
    self._in_queue.put(None)
    self._proc.join()

  def enqueue_work(self, work_id, work_ctor):
    """Submit work to this worker's queue.

    Args:
      work_id: Unique identifier for the work item.
      work_ctor: Callable accepting 'context' kwarg, returning awaitable.
    """
    self._in_queue.put(Work(id=work_id, ctor=work_ctor))


class AsyncManager:
  """Pool of worker processes for parallel async task execution.

  Distributes work across workers using a simple least-loaded strategy.
  Each worker runs an independent asyncio event loop in its own process.
  """

  def __init__(self, num_workers=None, mpctx=multiprocessing):
    """Initialize the async manager with a worker pool.

    Args:
      num_workers: Number of worker processes (default: CPU count).
      mpctx: Multiprocessing context (default: multiprocessing).
    """
    num_workers = num_workers or os.cpu_count()

    self._out_queue = mpctx.Queue()
    self._workers = [_Worker(mpctx, i, self._out_queue) for i in range(num_workers)]
    self._lock = threading.Lock()
    self._queued = np.zeros(num_workers, dtype=np.int64)

  def close(self):
    """Stop all worker processes."""
    for worker in self._workers:
      worker.stop()

  def enqueue_work(self, work_id, work_ctor):
    """Submit work to the least-loaded worker.

    Args:
      work_id: Unique identifier for the work item.
      work_ctor: Callable accepting 'context' kwarg, returning awaitable.
    """
    with self._lock:
      wid = np.argmin(self._queued)
      self._queued[wid] += 1
      worker = self._workers[wid]

    worker.enqueue_work(work_id, work_ctor)

  def fetch_result(self, block=True, timeout=None):
    """Fetch a completed work result.

    Args:
      block: Whether to block until a result is available (default: True).
      timeout: Maximum time to wait in seconds (default: None).

    Returns:
      Tuple of (work_id, result) or None if timeout and non-blocking.

    Raises:
      queue.Empty: If block=False and no result available.
    """
    try:
      wid, work_id, result = self._out_queue.get(block=block, timeout=timeout)

      with self._lock:
        self._queued[wid] -= 1

      return work_id, result
    except queue.Empty:
      pass

  def __enter__(self):
    """Context manager entry."""
    return self

  def __exit__(self, *exc):
    """Context manager exit: stop all workers."""
    self.close()

    return False


class AsyncRunner:
  """Single-threaded asyncio event loop running in a background thread.

  Provides a way to run async coroutines from synchronous code.
  Automatically registers for cleanup on process exit.
  """

  def __init__(self):
    """Initialize and start the background thread with event loop."""
    self._loop = asyncio.new_event_loop()
    self._thread = threading.Thread(target=self._async_runner, daemon=True)
    self._thread.start()
    self._cid = cleanups.register(self.stop)

  def _async_runner(self):
    """Thread target: run the event loop forever."""
    asyncio.set_event_loop(self._loop)
    self._loop.run_forever()

  def stop(self):
    """Stop the event loop and join the thread."""
    self._loop.call_soon_threadsafe(self._loop.stop)
    self._thread.join()

  def run(self, coro):
    """Submit a coroutine to the event loop.

    Args:
      coro: Coroutine to execute.

    Returns:
      concurrent.futures.Future for the coroutine result.
    """
    return asyncio.run_coroutine_threadsafe(coro, self._loop)


_ASYNC_RUNNER = gns.Var(f'{__name__}.ASYNC_RUNNER',
                        fork_init=True,
                        defval=lambda: AsyncRunner())


def _async_runner():
  """Get the global AsyncRunner instance."""
  return gns.get(_ASYNC_RUNNER)


def run_async(coro):
  """Submit a coroutine to the global async runner.

  Args:
    coro: Coroutine to execute.

  Returns:
    concurrent.futures.Future for the coroutine result.
  """
  return _async_runner().run(coro)
