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

  def __init__(self, proc, logfd=None):
    self._proc = proc
    self._logfd = logfd
    self._sent = set()

  def __call__(self, sig, frame):
    if sig not in self._sent:
      self._sent.add(sig)
      alog.async_log(alog.WARNING,
                     f'{signal.strsignal(sig)} received. Forwarding it to running ' \
                     f'child {proc.pid} ...',
                     file=self._logfd)

      self._proc.send_signal(sig)

    return sgn.HANDLED


class _Writer:

  def __init__(self, fd):
    self._fd = fd
    self._is_binary = fsu.is_binary(fd)

  def write(self, data):
    self._fd.write(data if self._is_binary else data.decode())
    self._fd.flush()


class _Reader:

  def __init__(self, fd):
    self._read = getattr(fd, 'read1', getattr(fd, 'readline', None))

  def read(self):
    return self._read()


def _lookup(tmpl_envs, key, defval=None):
  for env in tmpl_envs:
    value = env.get(key)
    if value is not None:
      return value

  alog.xraise(KeyError, f'Unable to lookup "{key}" while substituting command ' \
              f'line arguments')


def run(cmd, outfd=None, tmpl_envs=None, **kwargs):
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
  with sgn.Signals('INT, TERM', _SigHandler(proc, logfd=outfd)):
    while True:
      data = reader.read()
      if data:
        writer.write(data)
      elif proc.poll() is not None:
        break

  return proc.returncode

