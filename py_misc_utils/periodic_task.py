import threading

from . import alog as alog
from . import scheduler as sch


class PeriodicTask:

  def __init__(self, name, periodic_fn, period, scheduler=None, stop_on_error=None):
    self._name = name
    self._periodic_fn = periodic_fn
    self._period = period
    self._scheduler = scheduler or sch.common_scheduler()
    self._stop_on_error = stop_on_error in (None, True)
    self._lock = threading.Lock()
    self._event = None

  def start(self):
    with self._lock:
      if self._event is None:
        self._event = self._scheduler.enter(self._period, self._runner)

  def stop(self):
    with self._lock:
      if self._event is not None:
        self._scheduler.cancel(self._event)
        self._event = None

  def _runner(self):
    re_issue = True
    try:
      with self._lock:
        if self._event is not None:
          self._periodic_fn()
    except Exception as ex:
      alog.exception(ex, exmsg=f'Exception while running periodic task "{self._name}"')
      re_issue = not self._stop_on_error
    finally:
      with self._lock:
        if self._event is not None:
          if re_issue:
            self._event = self._scheduler.enter(self._period, self._runner)
          else:
            self._event = None

