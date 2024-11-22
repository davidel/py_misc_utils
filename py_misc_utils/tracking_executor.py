import threading
import weakref

from . import alog
from . import cond_waiter as cwait
from . import executor as xe


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
    self._pending_cv = threading.Condition(lock=self._lock)

  def _report_done(self, tid):
    with self._lock:
      self._pending.remove(tid)
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
      self._report_done(tid)
      raise

    return tid

  def submit_result(self, fn, *args, **kwargs):
    wfn, tid = self._wrap(fn, *args, **kwargs)
    try:
      return self.executor.submit_result(wfn)
    except Exception:
      self._report_done(tid)
      raise

  def shutdown(self):
    self.executor.shutdown()
    self.wait()

  def wait(self, tids=None, timeout=None, timegen=None, waiter=None):
    waiter = waiter or cwait.CondWaiter(timeout=timeout, timegen=timegen)
    if not tids:
      with self._lock:
        while self._pending:
          if not waiter.wait(self._pending_cv):
            break

        return not self._pending
    else:
      stids = set(tids)
      with self._lock:
        while True:
          rem = stids & self._pending
          if not (rem and waiter.wait(self._pending_cv)):
            break

        return not rem

  def wait_for_idle(self, timeout=None, timegen=None):
    waiter = cwait.CondWaiter(timeout=timeout, timegen=timegen)

    return self.wait(waiter=waiter) and self.executor.wait_for_idle(waiter=waiter)

