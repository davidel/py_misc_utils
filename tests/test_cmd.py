"""Tests for py_misc_utils.cmd.

Expectations pinned to the observed behavior: run() executes a child
process as an argument list, forwards its stdout, and returns the exit
code; string commands are template-substituted and shell-split.
"""

import io
import os

from py_misc_utils import cmd


def test_run_arg_list():
  """run() executes an argument list and returns the exit code."""
  outfd = io.BytesIO()
  rc = cmd.run(['python3', '-c', 'print("hello cmd")'], outfd=outfd)
  assert rc == 0
  assert b'hello cmd' in outfd.getvalue()


def test_run_string_command(tmp_path):
  """run() accepts a template string command."""
  script = os.path.join(tmp_path, 'script.py')
  with open(script, 'w') as fd:
    fd.write('print("script output")')

  outfd = io.BytesIO()
  rc = cmd.run(f'python3 {script}', outfd=outfd)
  assert rc == 0
  assert b'script output' in outfd.getvalue()


def test_run_nonzero_exit():
  """run() returns nonzero exit codes."""
  outfd = io.BytesIO()
  rc = cmd.run(['python3', '-c', 'import sys; sys.exit(3)'], outfd=outfd)
  assert rc == 3


def test_template_substitution(monkeypatch):
  """Template variables in string commands are substituted."""
  monkeypatch.setenv('CMD_MSG', 'env_value')
  outfd = io.BytesIO()
  rc = cmd.run('echo $CMD_MSG', outfd=outfd)
  assert rc == 0
  assert b'env_value' in outfd.getvalue()
