import subprocess
import sys


def run(cmd, outfd=None, **kwargs):
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

