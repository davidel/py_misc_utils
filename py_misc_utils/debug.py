import pdb
import signal
import sys


def _debug(signum, frame):
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
  signum = getattr(signal, signame) if isinstance(signum, str) else signum

  signal.signal(signum, _debug)

