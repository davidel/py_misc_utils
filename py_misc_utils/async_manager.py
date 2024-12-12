import asyncio
import multiprocessing
import os
import queue
import threading

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

    if needs_raise and rexc[1] is not None:
      raise rexc[1]

  async def __aenter__(self):
    return self

  async def __aexit__(self, *exc):
    await self.close(*exc)

    return False


class _Worker:

  def __init__(self, task_ctor, out_queue):
    self._task_ctor = task_ctor
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
    task = self._task_ctor(context, work)

    result = await task

    self._out_queue.put((work, result))

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

  def enqueue_work(self, work):
    self._in_queue.put(work)


class AsyncManager:

  def __init__(self, task_ctor, num_workers=None):
    num_workers = num_workers or os.cpu_count()

    self._out_queue = multiprocessing.Queue()
    self._workers = [_Worker(task_ctor, self._out_queue) for _ in range(num_workers)]
    self._lock = threading.Lock()
    self._next_in = 0

  def close(self):
    for worker in self._workers:
      worker.stop()

  def enqueue_work(self, work):
    with self._lock:
      worker = self._workers[self._next_in]
      self._next_in = (self._next_in + 1) % len(self._workers)

    worker.enqueue_work(work)

  def fetch_result(self):
    return self._out_queue.get()


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

