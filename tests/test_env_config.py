"""Tests for py_misc_utils.env_config.EnvConfig.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): subclasses declare typed class attributes; EnvConfig
reads each variable from the uppercase-named environment variable (via
ut.getenv honoring the attribute type), registers a --<name> argparse
argument, and lets CLI values override env values. Missing env vars
leave the class default. repr summarizes all non-private attributes.
Environment isolation is provided via monkeypatch; CLI args are parsed
from sys.argv, so tests patch argv directly.
"""

import sys

import pytest

from py_misc_utils import env_config as ec


class Config(ec.EnvConfig):
  timeout = 30
  retries = 3
  name = 'base'
  ratio = 1.5


class FlagConfig(ec.EnvConfig):
  verbose = False


@pytest.fixture
def clean_env(monkeypatch):
  """Remove all the config-related env vars.

  Env var names are the raw attribute names (lowercase), as read by
  ut.getenv(name) inside EnvConfig.__init__.
  """
  for name in ('timeout', 'retries', 'name', 'ratio', 'verbose'):
    monkeypatch.delenv(name, raising=False)

  return monkeypatch


def _run_with_argv(argv, cls=None):
  """Instantiate the config with a patched sys.argv for CLI parsing."""
  old_argv = sys.argv
  sys.argv = ['prog'] + argv
  try:
    return cls() if cls is not None else Config()
  finally:
    sys.argv = old_argv


def test_defaults_when_no_env_or_cli(clean_env):
  """Class defaults survive when no env var and no CLI arg is given."""
  cfg = _run_with_argv([])
  assert cfg.timeout == 30
  assert cfg.retries == 3
  assert cfg.name == 'base'


def test_env_vars_set_values(clean_env):
  """Env vars named after the attribute populate the config attributes."""
  clean_env.setenv('timeout', '60')
  clean_env.setenv('retries', '7')

  cfg = _run_with_argv([])

  assert cfg.timeout == 60
  assert cfg.retries == 7
  assert cfg.name == 'base'


def test_env_var_typed_parsing(clean_env):
  """Numeric and float env vars are parsed into the declared types."""
  clean_env.setenv('ratio', '2.25')

  cfg = _run_with_argv([])

  assert cfg.ratio == 2.25
  assert isinstance(cfg.timeout, int)


def test_cli_overrides_env(clean_env):
  """A --<name> CLI argument wins over the env var value."""
  clean_env.setenv('timeout', '60')

  cfg = _run_with_argv(['--timeout', '99'])

  assert cfg.timeout == 99


def test_cli_only(clean_env):
  """CLI args work without any env var set."""
  cfg = _run_with_argv(['--name', 'cli'])

  assert cfg.name == 'cli'


def test_cli_bool_flag(clean_env):
  """A bool attribute becomes a typed CLI argument taking a value."""
  cfg = _run_with_argv(['--verbose', 'True'], FlagConfig)

  assert cfg.verbose is True


def test_repr(clean_env):
  """repr summarizes every non-private attribute."""
  cfg = _run_with_argv([])
  text = repr(cfg)

  assert 'timeout' in text
  assert 'ratio' in text
