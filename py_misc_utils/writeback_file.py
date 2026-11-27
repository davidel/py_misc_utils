"""Files whose content is written back on close.

This module provides WritebackFile, a wrapper around a stream which
invokes a user provided callback -- typically to persist the stream
content -- when the file is closed.
"""

import functools

from . import fin_wrap as fw
from . import mirror_from as mrf


def _writeback(stream, writeback_fn):
  """Invoke the writeback callback and ensure the stream gets closed.

  Args:
    stream: Wrapped stream passed to the writeback callback.
    writeback_fn: Callback invoked with the stream to persist its
      content.
  """
  try:
    writeback_fn(stream)
  finally:
    stream.close()


class WritebackFile:
  """A stream wrapper which persists its content on close.

  The wrapped stream is closed by invoking the configured writeback
  function -- typically uploading or saving the data -- either
  explicitly via close(), on context exit, or when the wrapper is
  garbage collected.

  Example:
    import io

    from py_misc_utils import writeback_file as wbf

    src = io.StringIO()
    saved = []

    with wbf.WritebackFile(src, lambda stream: saved.append(stream.getvalue())) as wf:
      wf.write('hello')

    print(saved)  # ['hello']
    print(src.closed)  # True

    # detach() closes the stream without running the writeback:
    src2 = io.StringIO()
    dropped = []

    wf2 = wbf.WritebackFile(src2, lambda stream: dropped.append(stream.getvalue()))
    wf2.detach()

    print(dropped)  # []
    print(src2.closed)  # True
  """

  def __init__(self, stream, writeback_fn):
    """Initialize the writeback file.

    Args:
      stream: Wrapped stream to read from and mirror methods from.
      writeback_fn: Callback invoked with the stream when it is closed
        with writeback enabled.
    """
    self._writeback_fn = functools.partial(_writeback, stream, writeback_fn)
    fw.fin_wrap(self, '_stream', stream, finfn=self._writeback_fn)
    mrf.mirror_all(stream, self, name='stream')

  def close(self, run_writeback=True):
    """Close the wrapper, optionally running the writeback callback.

    Args:
      run_writeback: If True, invoke the writeback callback with the
        stream; otherwise just close the stream, discarding the
        writeback.
    """
    stream = self._stream
    if stream is not None:
      fw.fin_wrap(self, '_stream', None)
      mrf.unmirror(self, name='stream')
      if run_writeback:
        self._writeback_fn()
      else:
        stream.close()

  def detach(self):
    """Close the wrapper without running the writeback callback."""
    self.close(run_writeback=False)

  def __enter__(self):
    """Return the writeback file as a context manager object."""
    return self

  def __exit__(self, exc_type, *exc_args):
    """Close the file, running the writeback only on clean exit."""
    self.close(run_writeback=exc_type is None)

    return False
