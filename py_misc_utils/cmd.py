import functools
import os
import shlex
import signal
import string
import subprocess
import sys

from . import alog
from . import signal as sgn
from . import template_replace as tr


class _SigHandler:

  def __init__(self, proc):
    self._proc = proc
    self._sent = set()

  def __call__(self, sig, frame):
    if sig not in self._sent:
      self._sent.add(sig)
      alog.async_log(alog.WARNING,
                     f'{signal.strsignal(sig)} received. Forwarding it to running ' \
                     f'child {proc.pid} ...')

      self._proc.send_signal(sig)

    return sgn.HANDLED


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

  outfd = outfd or sys.stdout

  alog.debug(f'Running: {cmd}')

  proc = subprocess.Popen(cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          **kwargs)

  readfn = getattr(proc.stdout, 'read1', None)
  if readfn is None:
    readfn = getattr(proc.stdout, 'readline', None)

  with sgn.Signals('INT, TERM', _SigHandler(proc)):
    while True:
      data = readfn()
      if data:
        outfd.write(data)
        outfd.flush()
      elif proc.poll() is not None:
        break

  return proc.returncode

