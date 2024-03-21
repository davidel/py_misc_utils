import threading
import weakref

from . import abs_timeout as abst
from . import executor as xe
from . import timegen as tg


def _wrap_task(executor, tid, fn, *args, **kwargs):
  eref = weakref.ref(executor)
  del executor

  def wfn():
    try:
      return fn(*args, **kwargs)
    finally:
      xtor = eref()
      if xtor is not None:
        xtor._report_done(tid)

  return wfn


class TrackingExecutor:

  def __init__(self, executor=None):
    self.executor = executor if executor is not None else xe.common_executor()
    self._lock = threading.Lock()
    self._task_id = 0
    self._pending = set()
    self._pending_cv = threading.Condition(self._lock)

  def _report_done(self, tid):
    with self._lock:
      self._pending.remove(tid)
      if not self._pending:
        self._pending_cv.notify_all()

  def _wrap(self, fn, *args, **kwargs):
    with self._lock:
      wfn = _wrap_task(self, self._task_id, fn, *args, **kwargs)
      self._pending.add(self._task_id)
      self._task_id += 1

      return wfn, self._task_id - 1

  def submit(self, fn, *args, **kwargs):
    wfn, tid = self._wrap(fn, *args, **kwargs)
    try:
      self.executor.submit(wfn)
    except Exception:
      with self._lock:
        self._pending.remove(tid)

      raise

  def submit_result(self, fn, *args, **kwargs):
    wfn, tid = self._wrap(fn, *args, **kwargs)
    try:
      return self.executor.submit_result(wfn)
    except Exception:
      with self._lock:
        self._pending.remove(tid)

      raise

  def shutdown(self):
    self.executor.shutdown()
    self.wait()

  def wait(self, timeout=None, timegen=None):
    atimegen = tg.TimeGen() if timegen is None else timegen
    atimeo = abst.AbsTimeout(timeout, timefn=atimegen.now)
    with self._lock:
      while self._pending:
        atimegen.wait(self._pending_cv, timeout=atimeo.get())

