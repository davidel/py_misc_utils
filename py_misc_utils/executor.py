import collections
import heapq
import os
import threading
import time
import weakref

from . import alog


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
      alog.exception(e, exmsg=f'Exception while running scheduled task')
      fnres = e

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

      return self.result


class _Queue:

  def __init__(self):
    self.lock = threading.Lock()
    self.cond = threading.Condition(lock=self.lock)
    self.queue = collections.deque()

  def put(self, task):
    with self.lock:
      self.queue.append(task)
      self.cond.notify()

      return len(self.queue)

  def get(self, timeout=None):
    with self.lock:
      while not self.queue:
        if not self.cond.wait(timeout=timeout):
          break

      return self.queue.popleft() if self.queue else None

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

  def _register(self):
    executor = self.executor()
    if executor is not None:
      executor._register_worker(self)

  def _unregister(self):
    executor = self.executor()
    if executor is not None:
      executor._unregister_worker(self)

  def _run(self):
    self._register()

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


class Executor:

  def __init__(self, max_threads=None, min_threads=None, name_prefix=None,
               idle_timeout=None):
    self._max_threads = max_threads or os.cpu_count()
    self._min_threads = min_threads or max(1, self._max_threads // 4)
    self._name_prefix = name_prefix or 'Executor'
    self._idle_timeout = idle_timeout or 5
    self._lock = threading.Lock()
    self._queue = _Queue()
    self._workers = dict()
    self._num_threads, self._thread_counter = 0, 0
    self._shutdown = False

  def _register_worker(self, worker):
    alog.spam(f'Registering worker thread {worker.ident}')
    with self._lock:
      self._workers[worker.ident] = worker

  def _unregister_worker(self, worker):
    alog.spam(f'Unregistering worker thread {worker.ident}')
    with self._lock:
      self._workers.pop(worker.ident, None)
      self._num_threads -= 1

  def _new_name(self):
    self._thread_counter += 1

    return f'{self._name_prefix}-{self._thread_counter}'

  def _maybe_add_worker(self, queued):
    if ((queued > 1 and self._num_threads < self._max_threads) or
        self._num_threads < self._min_threads):
      # Up to min_threads the workers should never quit, so they get None as
      # timeout, while the one after that will get _idle_timeout which will
      # make them quit if no task is fetched within such timeout.
      idle_timeout = self._idle_timeout if self._num_threads > self._min_threads else None

      worker = _Worker(weakref.ref(self), self._queue, self._new_name(),
                       idle_timeout=idle_timeout)

      self._num_threads += 1

      alog.spam(f'New thread #{self._num_threads} with ID {worker.ident}')

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
      for _ in range(self._num_threads):
        self._queue.put(None)

      workers = tuple(self._workers.values())

    alog.spam(f'Waiting {len(workers)} worker threads to complete')
    for worker in workers:
      worker.join()

