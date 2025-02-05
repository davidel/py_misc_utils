import signal

from . import core_utils as cu
from . import signal as sgn


class NoBreak:

  SIGMAP = {
    'INT': signal.SIGINT,
    'TERM': signal.SIGTERM,
  }

  def __init__(self, sigs=None, exit_trigger=False):
    if sigs is None:
      self._signals = tuple(self.SIGMAP.values())
    else:
      if isinstance(sigs, str):
        sigs = cu.splitstrip(sigs, ',')

      self._signals = tuple(self.SIGMAP[sig] for sig in sigs)

    self._exit_trigger = exit_trigger

  def __enter__(self):
    self._signal_received = []
    for sig in self._signals:
      sgn.signal(sig, self._handler, prio=sgn.STD_PRIO)

    return self

  def _handler(self, sig, frame):
    self._signal_received.append((sig, frame))

    return sgn.HANDLED

  def __exit__(self, *exc):
    for sig in self._signals:
      sgn.unsignal(sig, self._handler)

    if self._exit_trigger:
      for sig, frame in self._signal_received:
        sgn.trigger(sig, frame)

    return False

