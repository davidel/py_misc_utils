import inspect
import string
import subprocess
import sys


def run(cmd, outfd=None, tmpl_env=None, , **kwargs):
  if isinstance(cmd, str):
    tmpl_env = tmpl_env or inspect.currentframe().f_back.f_globals
    cmd = string.Template(cmd).substitute(tmpl_env).split()

  outfd = outfd or sys.stdout

  proc = subprocess.Popen(cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          **kwargs)

  readfn = getattr(proc.stdout, 'read1', None)
  if readfn is None:
    readfn = getattr(proc.stdout, 'readline', None)

  while True:
    data = readfn()
    if data:
      outfd.write(data)
      outfd.flush()
    elif proc.poll() is not None:
      break

  return proc.returncode

