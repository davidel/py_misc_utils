import threading
import weakref

from . import alog
from . import scheduler as sch
from . import weak_call as wcall


class PeriodicTask:

  def __init__(self, name, periodic_fn, period, scheduler=None, stop_on_error=None):
    self._name = name
    self._periodic_fn = wcall.WeakCall(periodic_fn)
    self._period = period
    self._scheduler = scheduler or sch.common_scheduler()
    self._stop_on_error = stop_on_error in (None, True)
    self._lock = threading.Lock()
    self._event = None
    self._completed_event = None

  def start(self):
    with self._lock:
      if self._event is None:
        self._schedule()

    return self

  def stop(self):
    completed_event = None
    with self._lock:
      if self._event is not None:
        # If "events" is empty, we were not able to cancel the task, so it will be
        # in flight, and we need to wait for it to complete before exiting.
        events = self._scheduler.cancel(self._event)
        completed_event = self._completed_event if not events else None
        self._event = None

    if completed_event is not None:
      completed_event.wait()

  def _schedule(self):
    self._event = self._scheduler.enter(self._period, self._runner)
    self._completed_event = threading.Event()

  def _runner(self):
    re_issue = True
    try:
      if self._periodic_fn() is wcall.GONE:
        re_issue = False
    except Exception as ex:
      alog.exception(ex, exmsg=f'Exception while running periodic task "{self._name}"')
      re_issue = not self._stop_on_error
    finally:
      with self._lock:
        self._completed_event.set()
        if self._event is not None:
          if re_issue:
            self._schedule()
          else:
            self._event = None

