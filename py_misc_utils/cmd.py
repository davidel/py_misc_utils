import os
import shlex
import signal
import string
import subprocess
import sys

from . import alog
from . import inspect_utils as iu
from . import signal as sgn
from . import template_replace as tr


def _handler(proc):
  def sig_handler(sig, frame):
    proc.send_signal(sig)
    return sgn.HANDLED

  return sig_handler


def _lookup_fn(tmpl_env, use_environ):
  def lookup(key, defval=None):
    value = tmpl_env.get(key)
    if value is None and use_environ:
      value = os.environ.get(key)

    return value

  return lookup


def run(cmd, outfd=None, tmpl_env=None, use_environ=None, **kwargs):
  if isinstance(cmd, str):
    tmpl_env = tmpl_env or iu.parent_globals()
    use_environ = False if use_environ is None else use_environ
    cmd = shlex.split(tr.template_replace(cmd,
                                          lookup_fn=_lookup_fn(tmpl_env, use_environ)))

  outfd = outfd or sys.stdout

  alog.debug(f'Running: {cmd}')

  proc = subprocess.Popen(cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          **kwargs)

  readfn = getattr(proc.stdout, 'read1', None)
  if readfn is None:
    readfn = getattr(proc.stdout, 'readline', None)

  with sgn.Signals((signal.SIGINT, signal.SIGTERM), _handler(proc)):
    while True:
      data = readfn()
      if data:
        outfd.write(data)
        outfd.flush()
      elif proc.poll() is not None:
        break

  return proc.returncode

