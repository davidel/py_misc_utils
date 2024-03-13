import collections
import concurrent.futures
import heapq
import os
import threading
import time
import uuid

from . import alog
from . import utils as ut


Event = collections.namedtuple(
    'Event',
    'time, sequence, ref, action, argument, kwargs')


class TimeGen:

  def now(self):
    return time.time()

  def wait(self, cond, timeout=None):
    cond.wait(timeout=timeout)


class Scheduler:

  def __init__(self, timegen=None, executor=None, max_workers=None, name='Scheduler'):
    self._queue = []
    self._sequence = 0
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self._timegen = TimeGen() if timegen is None else timegen
    self._executor = (executor if executor is not None else
                      concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_workers,
                        thread_name_prefix=name))
    self._runner = threading.Thread(target=self._run, daemon=True)
    self._runner.start()

  @property
  def timegen(self):
    return self._timegen

  @property
  def executor(self):
    return self._executor

  def _run_event(self, event):
    try:
      event.action(*event.argument, **event.kwargs)
    except Exception as e:
      alog.exception(e, exmsg=f'Exception while running scheduled action')

  def _run(self):
    while True:
      now, event = self._timegen.now(), None
      with self._lock:
        timeout = (self._queue[0].time - now) if self._queue else None
        if timeout is None or timeout > 0:
          self._timegen.wait(self._cond, timeout=timeout)
        else:
          event = heapq.heappop(self._queue)

      if event is not None:
        self._executor.submit(self._run_event, event)

  def gen_unique_ref(self):
    return uuid.uuid4()

  def enterabs(self, ts, action, ref=None, argument=(), kwargs={}):
    with self._lock:
      event = Event(time=ts,
                    sequence=self._sequence,
                    ref=ref,
                    action=action,
                    argument=argument,
                    kwargs=kwargs)
      self._sequence += 1

      heapq.heappush(self._queue, event)
      if id(event) == id(self._queue[0]):
        self._cond.notify_all()

    return event

  def enter(self, delay, action, ref=None, argument=(), kwargs={}):
    return self.enterabs(self._timegen.now() + delay, action,
                         ref=ref,
                         argument=argument,
                         kwargs=kwargs)

  def _cancel_fn(self, fn):
    events = []
    with self._lock:
      pos = []
      for i, qe in enumerate(self._queue):
        if fn(qe):
          events.append(qe)
          pos.append(i)
      if pos:
        # Positions are added in asceending order above, here we pop them in
        # descending order to avoid invalidating positions.
        for i in range(len(pos) - 1, -1, -1):
          self._queue.pop(pos[i])

        heapq.heapify(self._queue)

    return events

  def cancel(self, event):
    if isinstance(event, (list, tuple)):
      ids = set([id(e) for e in event])
      return self._cancel_fn(lambda qe: id(qe) in ids)

    return self._cancel_fn(lambda qe: id(qe) == id(event))

  def ref_cancel(self, ref):
    if isinstance(ref, (list, tuple)):
      refs = set(ref)
      return self._cancel_fn(lambda qe: qe.ref in refs)

    return self._cancel_fn(lambda qe: qe.ref == ref)

  def get_events(self, fn):
    events = []
    with self._lock:
      for qe in self._queue:
        if fn(qe):
          events.append(qe)

    return events


_LOCK = threading.Lock()
_EXECUTOR = None
_SCHEDULER = None

def _common_executor():
  global _EXECUTOR

  if _EXECUTOR is None:
    _EXECUTOR = concurrent.futures.ThreadPoolExecutor(
      max_workers=ut.getenv('EXECUTOR_WORKERS', dtype=int),
      thread_name_prefix=os.getenv('EXECUTOR_NAME', 'Common Executor'),
    )

  return _EXECUTOR


def common_executor():
  with _LOCK:
    return _common_executor()


def common_scheduler():
  global _SCHEDULER

  with _LOCK:
    if _SCHEDULER is None:
      _SCHEDULER = Scheduler(executor=_common_executor())

    return _SCHEDULER

