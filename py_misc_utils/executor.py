import collections
import heapq
import os
import threading
import time
import weakref

from . import alog


_Task = collections.namedtuple('Task', 'id, fn, args, kwargs')


class _Void:
  pass

VOID = _Void()


class AsyncResult:

  def __init__(self):
    self.lock = threading.Lock()
    self.cond = threading.Condition(lock=self.lock)
    self.result = VOID

  def set(self, result):
    with self.lock:
      self.result = result
      self.cond.notify_all()

  def wait(self, timeout=None):
    with self.lock:
      while self.result is VOID:
        if not self.cond.wait(timeout=timeout):
          break

      return self.result


class _Queue:

  def __init__(self):
    self.lock = threading.Lock()
    self.cond = threading.Condition(lock=self.lock)
    self.queue = collections.deque()

  def put(self, task):
    with self.lock:
      self.queue.append(task)
      if task is None:
        self.cond.notify_all()
      else:
        self.cond.notify()

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

  def __init__(self, executor, queue, init_fn=None, idle_timeout=None):
    self.executor = executor
    self.queue = queue
    self.init_fn = init_fn
    self.idle_timeout = idle_timeout
    self.sync_queue = collections.deque()
    self.thread = threading.Thread(target=self._run, daemon=True)
    self.thread.start()

  def _run_task(self, task):
    try:
      task.fn(*task.args, **task.kwargs)
    except Exception as e:
      alog.exception(e, exmsg=f'Exception while running scheduled task')

    self._complete(task)

  def _register(self):
    executor = self.executor()
    if executor is not None:
      executor._register_worker(self)

  def _unregister(self):
    executor = self.executor()
    if executor is not None:
      executor._unregister_worker(self)

  def _complete(self, task):
    executor = self.executor()
    if executor is not None:
      executor._complete(task)

  def _run(self):
    if self.init_fn is not None:
      self.init_fn()
      self.init_fn = None

    self._register()

    while True:
      if self.sync_queue:
        task = self.sync_queue.popleft()
      else:
        task = self.queue.get(timeout=self.idle_timeout)

      if task is None:
        break

      self._run_task(task)

    self._unregister()

  @property
  def ident(self):
    return self.thread.ident


def _wrap_init_fn(init_fn):
  ares = AsyncResult()

  def fn():
    try:
      ares.set(fn())
    except Exception as e:
      alog.exception(e, exmsg=f'Exception while running executor thread init function')
      ares.set(e)
      raise

  return ares, fn


class Executor:

  def __init__(self, max_threads=None, min_threads=None, init_fn=None,
               idle_timeout=None):
    self._max_threads = max_threads or os.cpu_count()
    self._min_threads = min_threads or max(1, self._max_threads // 4)
    self._init_fn = init_fn
    self._idle_timeout = idle_timeout or 5
    self._lock = threading.Lock()
    self._completed_cond = threading.Condition(lock=self._lock)
    self._queue = _Queue()
    self._workers = dict()
    self._id = 0
    self._pending = dict()

  def _register_worker(self, worker):
    with self._lock:
      self._workers[worker.ident] = worker

  def _unregister_worker(self, worker):
    with self._lock:
      rworker = self._workers.pop(worker.ident, None)
      if worker is not rworker:
        # Should not happen ...
        self._workers[rworker.ident] = rworker

  def _complete(self, task):
    with self._lock:
      self._pending.pop(task.id)
      self._completed_cond.notify_all()

  def _maybe_add_worker(self):
    num_workers = len(self._workers)
    if ((len(self._queue) > 0 and num_workers < self._max_threads) or
        num_workers < self._min_threads):
      idle_timeout = self._idle_timeout if num_workers > self._min_threads else None
      if self._init_fn:
        ares, init_fn = _wrap_init_fn(self._init_fn)
      else:
        ares, init_fn = None, None

      worker = _Worker(weakref.ref(self), self._queue,
                       init_fn=init_fn,
                       idle_timeout=idle_timeout)

      if ares is not None:
        init_result = ares.wait()
        if isinstance(init_result, Exception):
          raise init_result

      alog.debug0(f'New thread #{num_workers} with ID {worker.ident}')

  def submit(self, fn, *args, sync=False, **kwargs):
    with self._lock:
      self._maybe_add_worker()

      task = _Task(id=self._id, fn=fn, args=args, kwargs=kwargs)
      self._id += 1

      self._pending[task.id] = task
      if sync:
        worker = self._workers.get(threading.get_ident(), None)
        if worker is not None:
          worker.sync_queue.append(task)
        else:
          self._queue.put(task)
      else:
        self._queue.put(task)

      return task

  def wait_task(self, task, timeout=None):
    expires = time.time() + timeout if timeout is not None else None
    with self._lock:
      while task.id in self._pending:
        curr_timeout = expires - time.time() if expires is not None else None
        if not self._completed_cond.wait(timeout=curr_timeout):
          return False

      return True

  def task_barrier(self, task, timeout=None):
    expires = time.time() + timeout if timeout is not None else None
    with self._lock:
      while True:
        minid = min(self._pending.keys())
        if minid > task.id:
          break

        curr_timeout = expires - time.time() if expires is not None else None
        if not self._completed_cond.wait(timeout=curr_timeout):
          return False

      return True

  def stop(self):
    alog.debug0(f'Stopping executor')
    while True:
      with self._lock:
        if self._workers:
          self._queue.put(None)
        else:
          break

