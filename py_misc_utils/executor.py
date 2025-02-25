import collections
import heapq
import os
import threading
import time
import weakref

from . import alog
from . import cond_waiter as cwait
from . import utils as ut


_ExceptionWrapper = collections.namedtuple('ExceptionWrapper', 'exception')


class Task:

  def __init__(self, fn, args=None, kwargs=None, aresult=None):
    self._fn = fn
    self._args = args or ()
    self._kwargs = kwargs or dict()
    self._aresult = aresult

  def __call__(self):
    try:
      fnres = self._fn(*self._args, **self._kwargs)
    except Exception as ex:
      alog.exception(ex, exmsg=f'Exception while running task')
      fnres = _ExceptionWrapper(exception=ex)

    if self._aresult is not None:
      self._aresult.set(fnres)


class _Void:
  pass

VOID = _Void()

class AsyncResult:

  def __init__(self):
    self._cond = threading.Condition(lock=threading.Lock())
    self._result = VOID

  def set(self, result):
    with self._cond:
      self._result = result
      self._cond.notify_all()

  def wait(self, timeout=None):
    with self._cond:
      # No need for a loop here, as the condition is signaled only when result
      # is set by the producer.
      if self._result is VOID:
        self._cond.wait(timeout=timeout)

      if isinstance(self._result, _ExceptionWrapper):
        raise self._result.exception

      return self._result


class Queue:

  def __init__(self):
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self._queue = collections.deque()
    self._stopped = 0

  def put(self, task):
    with self._lock:
      self._queue.append(task)
      self._cond.notify()

      return len(self._queue)

  def get(self, timeout=None):
    with self._lock:
      while True:
        # Even in case of stopped queue, always return pending items if available.
        if self._queue:
          return self._queue.popleft()
        if self._stopped > 0 or not self._cond.wait(timeout=timeout):
          break

  def start(self):
    with self._lock:
      self._stopped -= 1

  def stop(self):
    with self._lock:
      self._stopped += 1
      self._cond.notify_all()

  def __len__(self):
    with self._lock:
      return len(self._queue)


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

      task()
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
    self._idle_cond = threading.Condition(lock=self._lock)

  def _unregister_worker(self, worker):
    alog.spam(f'Unregistering worker thread {worker.ident}')
    with self._lock:
      self._workers.pop(worker.ident, None)
      if not self._workers:
        self._idle_cond.notify_all()

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
      queued = self._queue.put(task)
      self._maybe_add_worker(queued)

  def submit(self, fn, *args, **kwargs):
    self._submit_task(Task(fn, args=args, kwargs=kwargs))

  def submit_result(self, fn, *args, **kwargs):
    aresult = AsyncResult()

    self._submit_task(Task(fn, args=args, kwargs=kwargs, aresult=aresult))

    return aresult

  def shutdown(self):
    alog.debug0(f'Stopping executor')
    self._queue.stop()
    with self._lock:
      alog.debug0(f'Waiting executor workers exit')
      while self._workers:
        self._idle_cond.wait()

  def wait_for_idle(self, timeout=None, timegen=None, waiter=None):
    alog.debug0(f'Waiting for idle ...')

    waiter = waiter or cwait.CondWaiter(timeout=timeout, timegen=timegen)
    self._queue.stop()
    try:
      with self._lock:
        while self._workers:
          if not waiter.wait(self._idle_cond):
            return False
    finally:
      self._queue.start()
      alog.debug0(f'Waiting for idle ... done')

    return True


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

