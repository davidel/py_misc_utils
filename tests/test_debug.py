"""Tests for py_misc_utils.debug.

Expectations pinned to the observed behavior: install_pdb_hook installs
a signal handler that enters pdb (structural test only — pdb cannot be
entered in automated tests). The hook resets the handler to SIG_DFL on
first signal so a repeated signal terminates the process.
"""

import signal as sgn

from py_misc_utils import debug


def _reset_sigusr1():
  """Reset SIGUSR1 to default to avoid leaking the pdb hook."""
  sgn.signal(sgn.SIGUSR1, sgn.SIG_DFL)


def test_install_pdb_hook_structural():
  """install_pdb_hook accepts a signal number and installs a handler."""
  debug.install_pdb_hook(sgn.SIGUSR1)
  _reset_sigusr1()


def test_install_pdb_hook_signal_name():
  """install_pdb_hook accepts a signal name string."""
  debug.install_pdb_hook('SIGUSR1')
  _reset_sigusr1()
