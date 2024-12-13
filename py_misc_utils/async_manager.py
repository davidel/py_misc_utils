import asyncio
import collections
import multiprocessing
import os
import queue
import threading

import numpy as np

from . import cleanups
from . import run_once as ro


class AsyncContext:

  def __init__(self):
    self._contexts = dict()

  async def add(self, name, context):
    result = await context.__aenter__()
    self._contexts[name] = (context, result)

    return result

  async def remove(self, name):
    context, _ = self._contexts.pop(name)
    await context.__aexit__(None, None, None)

  async def get(self, name, context_ctor):
    context_result = self._contexts.get(name)
    if context_result is None:
      result = await self.add(name, context_ctor())
    else:
      result = context_result[1]

    return result

  async def close(self, *exc):
    rexc = exc or (None, None, None)
    needs_raise = False
    for context, _ in self._contexts.values():
      exitres = await context.__aexit__(*rexc)
      needs_raise = needs_raise or not exitres

    self._contexts = dict()
    if needs_raise and rexc[1] is not None:
      raise rexc[1]

  async def __aenter__(self):
    return self

  async def __aexit__(self, *exc):
    await self.close(*exc)

    return False


Work = collections.namedtuple('Work', 'id, ctor')

class _Worker:

  def __init__(self, wid, out_queue):
    self._wid = wid
    self._out_queue = out_queue
    self._in_queue = multiprocessing.Queue()
    self._proc = multiprocessing.Process(target=self._run)
    self._proc.start()

  def _run(self):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    thread = threading.Thread(target=self._work_feeder, args=(loop,), daemon=True)
    thread.start()

    loop.run_forever()
    thread.join()

  async def _task_runner(self, context, work):
    try:
      task = work.ctor(context=context)

      result = await task
    except Exception as ex:
      result = ex

    self._out_queue.put((self._wid, work.id, result))

  def _work_feeder(self, loop):
    context = AsyncContext()

    while True:
      work = self._in_queue.get()
      if work is None:
        break

      asyncio.run_coroutine_threadsafe(self._task_runner(context, work), loop)

    asyncio.run_coroutine_threadsafe(self._shutdown(context, loop), loop)

  @classmethod
  async def _shutdown(cls, context, loop):
    await context.close()
    loop.stop()

  def stop(self):
    self._in_queue.put(None)
    self._proc.join()

  def enqueue_work(self, work_id, work_ctor):
    self._in_queue.put(Work(id=work_id, ctor=work_ctor))


class AsyncManager:

  def __init__(self, num_workers=None):
    num_workers = num_workers or os.cpu_count()

    self._out_queue = multiprocessing.Queue()
    self._workers = [_Worker(i, self._out_queue) for i in range(num_workers)]
    self._lock = threading.Lock()
    self._queued = np.zeros(num_workers, dtype=np.int64)

  def close(self):
    for worker in self._workers:
      worker.stop()

  def enqueue_work(self, work_id, work_ctor):
    with self._lock:
      wid = np.argmin(self._queued)
      self._queued[wid] += 1
      worker = self._workers[wid]

    worker.enqueue_work(work_id, work_ctor)

  def fetch_result(self, block=True, timeout=None):
    try:
      wid, work_id, result = self._out_queue.get(block=block, timeout=timeout)

      with self._lock:
        self._queued[wid] -= 1

      return work_id, result
    except queue.Empty:
      pass

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    self.close()

    return False


class AsyncRunner:

  def __init__(self):
    self._loop = asyncio.new_event_loop()
    self._thread = threading.Thread(target=self._async_runner, daemon=True)
    self._thread.start()

  def _async_runner(self):
    asyncio.set_event_loop(self._loop)
    self._loop.run_forever()

  def stop(self):
    self._loop.call_soon_threadsafe(self._loop.stop)
    self._thread.join()

  def run(self, coro):
    return asyncio.run_coroutine_threadsafe(coro, self._loop)


_ASYNC_RUNNER = None

@ro.run_once
def _create_async_runner():
  global _ASYNC_RUNNER

  _ASYNC_RUNNER = AsyncRunner()

  cleanups.register(_ASYNC_RUNNER.stop)


def run_async(coro):
  _create_async_runner()

  return _ASYNC_RUNNER.run(coro)

