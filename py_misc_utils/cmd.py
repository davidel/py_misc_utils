"""Execution of external commands with output forwarding.

This module provides a run function which starts a child process,
forwards its output to a file-like object and installs signal handlers
so that received termination signals are propagated to the child.
"""

import functools
import os
import shlex
import signal
import string
import subprocess
import sys

from . import alog
from . import fs_utils as fsu
from . import signal as sgn
from . import template_replace as tr


class _SigHandler:
  """Signal handler which forwards received signals to the child process.

  The first reception of each signal is logged and forwarded; subsequent
  receptions of the same signal are ignored.
  """

  def __init__(self, proc, logfd=None):
    """Initialize the signal handler.

    Args:
      proc: Child process to forward signals to.
      logfd: Optional file object to write signal log records to.
    """
    self._proc = proc
    self._logfd = logfd
    self._sent = set()

  def __call__(self, sig, frame):
    """Forward a received signal to the child process, once.

    Args:
      sig: Received signal number.
      frame: Current stack frame (unused).

    Returns:
      The HANDLED marker to acknowledge the signal.
    """
    if sig not in self._sent:
      self._sent.add(sig)
      alog.async_log(alog.WARNING,
                     f'{signal.strsignal(sig)} received. Forwarding it to running ' \
                     f'child {self._proc.pid} ...',
                     file=self._logfd)

      self._proc.send_signal(sig)

    return sgn.HANDLED


class _Writer:
  """Output sink which normalizes text/binary writes to the target file.

  The wrapped file is written as text or binary depending on its own
  capabilities, converting the data as needed.
  """

  def __init__(self, fd):
    """Initialize the writer.

    Args:
      fd: File object to write forwarded process output to.
    """
    self.fd = fd
    self._is_binary = fsu.is_binary(fd)

  def write(self, data):
    """Write process output data to the target file.

    Args:
      data: Bytes or string chunk read from the process output.
    """
    if self._is_binary:
      if isinstance(data, str):
        data = data.encode()
    elif not isinstance(data, str):
      data = data.decode()

    self.fd.write(data)
    self.fd.flush()


class _Reader:
  """Chunked reader over the child process output stream."""

  def __init__(self, fd):
    """Initialize the reader.

    Args:
      fd: File object to read process output from.
    """
    self._read = getattr(fd, 'read1', None)
    if self._read is None:
      self._read = getattr(fd, 'readline', None)

  def read(self):
    """Read one chunk of process output.

    Returns:
      A chunk of output data, or None when the process stream is
      exhausted.
    """
    return self._read()


def _lookup(tmpl_envs, key, defval=None):
  """Look up a template variable across the configured environments.

  Args:
    tmpl_envs: Iterable of mapping-like objects holding the template
      variables.
    key: Name of the variable to look up.
    defval: Unused default value placeholder.

  Returns:
    The value of the first environment defining the variable.

  Raises:
    KeyError: If no environment defines the requested variable.
  """
  for env in tmpl_envs:
    value = env.get(key)
    if value is not None:
      return value

  alog.xraise(KeyError, f'Unable to lookup "{key}" while substituting command ' \
              f'line arguments')


def run(cmd, outfd=None, tmpl_envs=None, **kwargs):
  """Run a child process, forwarding its output and signals.

  The command can be given as a string -- in which case template
  variables are substituted from the provided environments before shell
  splitting -- or as an argument list.

  Args:
    cmd: Command to run, as a template string or an argument list.
    outfd: Optional file object to forward the process output to;
      defaults to the standard output.
    tmpl_envs: Optional iterable of mappings used to resolve template
      variables in string commands.
    **kwargs: Extra arguments forwarded to subprocess.Popen.

  Returns:
    The exit code of the child process.
  """
  if isinstance(cmd, str):
    tmpl_envs = tmpl_envs or (os.environ,)
    cmd = tr.template_replace(cmd, lookup_fn=functools.partial(_lookup, tmpl_envs))
    cmd = shlex.split(cmd)

  alog.debug(f'Running: {cmd}')

  proc = subprocess.Popen(cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          **kwargs)

  reader = _Reader(proc.stdout)
  writer = _Writer(outfd or sys.stdout)
  with sgn.Signals('INT, TERM', _SigHandler(proc, logfd=writer.fd)):
    while True:
      data = reader.read()
      if data:
        writer.write(data)
      elif proc.poll() is not None:
        break

  return proc.returncode
