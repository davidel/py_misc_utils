import collections
import heapq
import os
import threading
import time
import weakref

from . import alog
from . import utils as ut


_ExceptionWrapper = collections.namedtuple('ExceptionWrapper', 'exception')


class Task:

  def __init__(self, fn, args, kwargs, aresult=None):
    self.fn = fn
    self.args = args
    self.kwargs = kwargs
    self.aresult = aresult

  def run(self):
    try:
      fnres = self.fn(*self.args, **self.kwargs)
    except Exception as e:
      alog.exception(e, exmsg=f'Exception while running executor task')
      fnres = _ExceptionWrapper(exception=e)

    if self.aresult is not None:
      self.aresult.set(fnres)


class _Void:
  pass

VOID = _Void()

class AsyncResult:

  def __init__(self):
    self.cond = threading.Condition(lock=threading.Lock())
    self.result = VOID

  def set(self, result):
    with self.cond:
      self.result = result
      self.cond.notify_all()

  def wait(self, timeout=None):
    with self.cond:
      # No need for a loop here, as the condition is signaled only when result
      # is set by the producer.
      if self.result is VOID:
        self.cond.wait(timeout=timeout)

      if isinstance(self.result, _ExceptionWrapper):
        raise self.result.exception

      return self.result


class Queue:

  def __init__(self):
    self.lock = threading.Lock()
    self.cond = threading.Condition(lock=self.lock)
    self.queue = collections.deque()
    self.closed = False

  def put(self, task):
    with self.lock:
      self.queue.append(task)
      self.cond.notify()

      return len(self.queue)

  def get(self, timeout=None):
    with self.lock:
      while True:
        if self.queue:
          return self.queue.popleft()
        if self.closed or not self.cond.wait(timeout=timeout):
          break

      return None

  def close(self):
    with self.lock:
      self.closed = True
      self.cond.notify_all()

  def __len__(self):
    with self.lock:
      return len(self.queue)


class _Worker:

  def __init__(self, executor, queue, name, idle_timeout=None):
    self.executor = executor
    self.queue = queue
    self.idle_timeout = idle_timeout
    self.thread = threading.Thread(target=self._run, name=name, daemon=True)
    self.thread.start()

  def _unregister(self):
    executor = self.executor()
    if executor is not None:
      executor._unregister_worker(self)

  def _run(self):
    while True:
      task = self.queue.get(timeout=self.idle_timeout)

      if task is None:
        break

      task.run()
      del task

    self._unregister()

  @property
  def ident(self):
    return self.thread.ident

  def join(self):
    self.thread.join()


def _compute_num_threads(min_threads, max_threads):
  if max_threads is None:
    max_threads = max(8, int(os.cpu_count() * 1.5))
  if min_threads is None:
    min_threads = max(2, max_threads // 4)

  return min_threads, max_threads


class Executor:

  def __init__(self, max_threads=None, min_threads=None, name_prefix=None,
               idle_timeout=None):
    self._min_threads, self._max_threads = _compute_num_threads(min_threads, max_threads)
    self._name_prefix = name_prefix or 'Executor'
    self._idle_timeout = idle_timeout or ut.getenv('EXECUTOR_IDLE_TIMEOUT', dtype=int, defval=5)
    self._lock = threading.Lock()
    self._queue = Queue()
    self._workers = dict()
    self._thread_counter = 0
    self._shutdown = False

  def _unregister_worker(self, worker):
    alog.spam(f'Unregistering worker thread {worker.ident}')
    with self._lock:
      self._workers.pop(worker.ident, None)

  def _new_name(self):
    self._thread_counter += 1

    return f'{self._name_prefix}-{self._thread_counter}'

  def _maybe_add_worker(self, queued):
    num_threads = len(self._workers)
    if ((queued > 1 and num_threads < self._max_threads) or num_threads < self._min_threads):
      # Up to min_threads the workers should never quit, so they get None as
      # timeout, while the one after that will get _idle_timeout which will
      # make them quit if no task is fetched within such timeout.
      idle_timeout = self._idle_timeout if num_threads > self._min_threads else None

      worker = _Worker(weakref.ref(self), self._queue, self._new_name(),
                       idle_timeout=idle_timeout)

      self._workers[worker.ident] = worker

      alog.spam(f'New thread #{num_threads} with ID {worker.ident}')

  def _submit_task(self, task):
    with self._lock:
      if self._shutdown:
        alog.xraise(RuntimeError, f'Cannot submit after shutdown!')

      queued = self._queue.put(task)
      self._maybe_add_worker(queued)

  def submit(self, fn, *args, **kwargs):
    self._submit_task(Task(fn, args, kwargs))

  def submit_result(self, fn, *args, **kwargs):
    aresult = AsyncResult()

    self._submit_task(Task(fn, args, kwargs, aresult=aresult))

    return aresult

  def shutdown(self):
    alog.spam(f'Stopping executor')

    with self._lock:
      self._shutdown = True
      workers = tuple(self._workers.values())

    self._queue.close()

    alog.spam(f'Waiting {len(workers)} worker threads to complete')
    for worker in workers:
      worker.join()


_LOCK = threading.Lock()
_EXECUTOR = None

def common_executor():
  global _EXECUTOR

  with _LOCK:
    if _EXECUTOR is None:
      _EXECUTOR = Executor(
        max_threads=ut.getenv('EXECUTOR_WORKERS', dtype=int),
        name_prefix=os.getenv('EXECUTOR_NAME', 'CommonExecutor'),
      )

    return _EXECUTOR

