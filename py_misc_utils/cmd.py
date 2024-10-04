import shlex
import signal
import string
import subprocess
import sys

from . import alog
from . import inspect_utils as iu
from . import signal as sgn


def _handler(proc):
  def sig_handler(sig, frame):
    proc.send_signal(sig)
    return sgn.HANDLED

  return sig_handler


def run(cmd, outfd=None, tmpl_env=None, **kwargs):
  if isinstance(cmd, str):
    tmpl_env = tmpl_env or iu.parent_globals()
    cmd = shlex.split(string.Template(cmd).substitute(tmpl_env))

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

