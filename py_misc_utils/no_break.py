import signal

from . import signal as sgn


class NoBreak:

  SIGMAP = {
    'INT': signal.SIGINT,
    'TERM': signal.SIGTERM,
  }

  def __init__(self, sigs=None):
    if sigs is None:
      self._signals = (signal.SIGINT, signal.SIGTERM,)
    else:
      self._signals = tuple(self.SIGMAP[s] for s in sigs.split(','))

  def __enter__(self):
    self._signal_received = []
    for sig in self._signals:
      sgn.signal(sig, self._handler, prio=sgn.MAX_PRIO)

    return self

  def _handler(self, sig, frame):
    self._signal_received.append((sig, frame))

    return sgn.HANDLED

  def __exit__(self, type, value, traceback):
    for sig in self._signals:
      sgn.unsignal(sig, self._handler)

    for sig, frame in self._signal_received:
      sgn.trigger(sig, frame)

