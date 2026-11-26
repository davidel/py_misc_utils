"""Thread-safe priority-queue based event scheduler.

This module provides :class:`Scheduler`, a sched-like class that executes
actions at scheduled times using a background thread and an executor.
Events can be scheduled at absolute or relative times, cancelled by event
or reference, and queried by predicate. A process-wide :func:`common_scheduler`
is also provided.
"""

import collections
import heapq
import threading
import uuid

from . import alog
from . import executor as xe
from . import fin_wrap as fw
from . import timegen as tg

Event = collections.namedtuple('Event', 'time, sequence, ref, action, argument, kwargs')


class Scheduler:
  """A thread-safe event scheduler executing actions via an executor.

  Events are stored in a heap ordered by (time, sequence). A daemon thread
  pops due events and submits them for execution.

  Attributes:
    timegen: The time source used for now() and wait().
    executor: The executor used to run scheduled actions.
  """

  def __init__(self, timegen=None, executor=None, max_workers=None, name='Scheduler'):
    """Initialize the Scheduler with a time source and executor.

    Args:
      timegen: Optional time source; defaults to a TimeGen created with name.
      executor: Optional executor for running actions; defaults to a new
        Executor with max_workers.
      max_workers: Maximum number of executor workers if executor not provided.
      name: Name used for the scheduler thread and executor.
    """
    self._queue = []
    self._sequence = 0
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self.timegen = tg.TimeGen() if timegen is None else timegen

    if executor is not None:
      self.executor = executor
    else:
      executor = xe.Executor(max_threads=max_workers, name_prefix=name)
      fw.fin_wrap(self, 'executor', executor, finfn=executor.shutdown)

    self._runner = threading.Thread(target=self._run, daemon=True)
    self._runner.start()

  def _run_event(self, event):
    """Execute a scheduled event's action, logging exceptions.

    Args:
      event: The Event instance to execute.
    """
    try:
      event.action(*event.argument, **event.kwargs)
    except Exception as ex:  # noqa: BLE001 -- action isolation: user-scheduled actions must not kill the scheduler worker; failure is logged
      alog.exception(ex, exmsg='Exception while running scheduled action')

  def _run(self):
    """Main scheduler loop: wait for due events and submit them for execution."""
    while True:
      now, event = self.timegen.now(), None
      with self._lock:
        timeout = (self._queue[0].time - now) if self._queue else None
        if timeout is None or timeout > 0:
          self.timegen.wait(self._cond, timeout=timeout)
        else:
          event = heapq.heappop(self._queue)

      if event is not None:
        self.executor.submit(self._run_event, event)

  def gen_unique_ref(self):
    """Generate a unique reference string for event cancellation.

    Returns:
      A UUID4-based string.
    """
    return str(uuid.uuid4())

  def enterabs(self, ts, action, ref=None, argument=(), kwargs=None):
    """Schedule an action at an absolute timestamp.

    Args:
      ts: The absolute time to run the action.
      action: The callable to execute.
      ref: Optional reference identifier for later cancellation.
      argument: Positional arguments for action.
      kwargs: Keyword arguments for action.

    Returns:
      The scheduled Event instance.
    """
    with self._lock:
      event = Event(time=ts,
                    sequence=self._sequence,
                    ref=ref,
                    action=action,
                    argument=argument,
                    kwargs=kwargs or {})
      self._sequence += 1

      heapq.heappush(self._queue, event)
      if id(event) == id(self._queue[0]):
        self._cond.notify()

    return event

  def enter(self, delay, action, ref=None, argument=(), kwargs=None):
    """Schedule an action after a relative delay.

    Args:
      delay: Seconds from now to run the action.
      action: The callable to execute.
      ref: Optional reference identifier for later cancellation.
      argument: Positional arguments for action.
      kwargs: Keyword arguments for action.

    Returns:
      The scheduled Event instance.
    """
    return self.enterabs(self.timegen.now() + delay,
                         action,
                         ref=ref,
                         argument=argument,
                         kwargs=kwargs)

  def _cancel_fn(self, fn):
    """Cancel all events matching the given predicate.

    Args:
      fn: A callable taking an Event and returning True if it should be cancelled.

    Returns:
      The list of cancelled Event instances.
    """
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
    """Cancel one or more scheduled events by identity.

    Args:
      event: An Event instance or list/tuple of Event instances.

    Returns:
      The list of cancelled Event instances.
    """
    # Note: Event is a namedtuple - hence a tuple - so a single event must
    # not be treated as a list of events.
    if isinstance(event, (list, tuple)) and not isinstance(event, Event):
      ids = set([id(e) for e in event])
      return self._cancel_fn(lambda qe: id(qe) in ids)

    return self._cancel_fn(lambda qe: id(qe) == id(event))

  def ref_cancel(self, ref):
    """Cancel events by their reference identifier.

    Args:
      ref: A reference string or list/tuple of reference strings.

    Returns:
      The list of cancelled Event instances.
    """
    if isinstance(ref, (list, tuple)):
      refs = set(ref)
      return self._cancel_fn(lambda qe: qe.ref in refs)

    return self._cancel_fn(lambda qe: qe.ref == ref)

  def get_events(self, fn):
    """Return queued events matching a predicate.

    Args:
      fn: A callable taking an Event and returning True if it matches.

    Returns:
      A list of matching Event instances currently in the queue.
    """
    events = []
    with self._lock:
      for qe in self._queue:
        if fn(qe):
          events.append(qe)

    return events


_LOCK = threading.Lock()
_SCHEDULER = None


def common_scheduler():
  """Return the process-wide common Scheduler instance, creating it if needed.

  Returns:
    The shared Scheduler backed by the common executor.
  """
  global _SCHEDULER

  with _LOCK:
    if _SCHEDULER is None:
      _SCHEDULER = Scheduler(executor=xe.common_executor())

    return _SCHEDULER
