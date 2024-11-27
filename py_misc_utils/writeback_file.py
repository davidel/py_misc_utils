import functools

from . import alog as alog
from . import assert_checks as tas
from . import fin_wrap as fw
from . import mirror_from as mrf


def _writeback(stream, writeback_fn):
  try:
    writeback_fn(stream)
  finally:
    stream.close()


@mrf.mirror_from(
  '_stream',
  (
    'closed',
    'fileno',
    'flush',
    'isatty',
    'read',
    'readall',
    'readinto',
    'readable',
    'readline',
    'readlines',
    'seek',
    'seekable',
    'tell',
    'truncate',
    'write',
    'writeable',
    'writelines',
  ),
)
class WritebackFile:

  def __init__(self, stream, writeback_fn):
    self._writeback_fn = writeback_fn
    fw.fin_wrap(self, '_stream', stream,
                finfn=functools.partial(_writeback, stream, writeback_fn))

  def close(self, run_writeback=True):
    stream = self._stream
    if stream is not None:
      fw.fin_wrap(self, '_stream', None)
      if run_writeback:
        _writeback(stream, self._writeback_fn)
      else:
        stream.close()

  def detach(self):
    self.close(run_writeback=False)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, *exc_args):
    self.close(run_writeback=exc_type is None)

    return False

