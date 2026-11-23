"""PDB debugging hook triggered by process signals.

This module installs a signal handler which, upon reception of a
configurable signal, enters the Python debugger (pdb) instead of
terminating the process.
"""

import pdb
import signal
import sys


def _debug(signum, frame):
  """Enter the debugger upon signal reception.

  Writes a help banner to stderr, resets the handler to the default
  disposition so that a repeated signal can still terminate (and
  possibly dump core), enters pdb, and restores this handler on exit.
  """
  signame = signal.strsignal(signum)
  sys.stderr.write(f'** {signame} received, entering debugger\n' \
                   f'** Type "c" to continue or "q" to stop the process\n' \
                   f'** Or {signame} again to quit (and possibly dump core)\n')
  sys.stderr.flush()

  signal.signal(signum, signal.SIG_DFL)
  try:
    pdb.set_trace()
  finally:
    signal.signal(signum, _debug)


def install_pdb_hook(signum):
  """Install the pdb hook for a signal.

  Args:
    signum: Signal number or name (e.g. 'USR1') to attach the hook to.
  """
  signum = getattr(signal, signum) if isinstance(signum, str) else signum

  signal.signal(signum, _debug)
