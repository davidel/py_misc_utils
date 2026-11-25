"""Tests for py_misc_utils.app_main.

Expectations pinned to the observed behavior: Main decorator converts
function parameters into command-line arguments; main() parses args and
invokes the main function.
"""

import sys
from unittest.mock import patch

from py_misc_utils import app_main


def test_main_converts_params_to_args(monkeypatch):
  """Main decorator adds CLI arguments matching function parameters."""
  called = {}

  @app_main.Main
  def my_main(count: int = 5, name: str = 'world'):
    called['count'] = count
    called['name'] = name

  parser = app_main.argparse.ArgumentParser()
  args = ['--count', '10', '--name', 'alice']
  with patch.object(sys, 'argv', ['prog'] + args):
    app_main.main(parser, my_main)

  assert called == {'count': 10, 'name': 'alice'}


def test_main_defaults_used(monkeypatch):
  """Default parameter values are used when no CLI args given."""
  called = {}

  @app_main.Main
  def my_main(count: int = 42):
    called['count'] = count

  parser = app_main.argparse.ArgumentParser()
  with patch.object(sys, 'argv', ['prog']):
    app_main.main(parser, my_main)

  assert called == {'count': 42}
