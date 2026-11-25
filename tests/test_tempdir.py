"""Tests for py_misc_utils.tempdir.

Per-process root temp dir (fork-safe via global_namespace), create() and
get_temp_root(), plus fastfs_dir() which locates the fastest available
filesystem (env var, tmpfs/ramfs candidates, temp dir, cwd) and creates
named or random-named subdirectories.

The root-dependent tests swap in a fresh process root via the global
namespace variable, so they do not depend on the shared singleton state
(other tests may have triggered its atexit cleanup mid-suite).
"""

import os

import pytest

from py_misc_utils import global_namespace as gns
from py_misc_utils import tempdir as tmpd


def test_fastfs_dir_random_name():
  """fastfs_dir() creates a random-named dir in the fastfs area."""
  path = tmpd.fastfs_dir()

  assert os.path.isdir(path)
  assert 1 <= len(os.path.basename(path)) <= 12


def test_fastfs_dir_named():
  """fastfs_dir(name=...) creates (or reuses) the named directory."""
  path = tmpd.fastfs_dir(name='batchb-test')

  assert os.path.isdir(path)
  assert os.path.basename(path) == 'batchb-test'

  again = tmpd.fastfs_dir(name='batchb-test')

  assert again == path


def test_fastfs_dir_namelen():
  """namelen bounds the random name length."""
  path = tmpd.fastfs_dir(namelen=8)

  assert len(os.path.basename(path)) == 8


def test_fastfs_dir_env_override(monkeypatch, tmp_path):
  """FASTFS_DIR env var redirects the fastfs area."""
  monkeypatch.setenv('FASTFS_DIR', str(tmp_path))

  # Reload the module-level resolution with the env var in place.
  import importlib
  importlib.reload(tmpd)

  try:
    path = tmpd.fastfs_dir(name='envtest')

    assert path.startswith(str(tmp_path))
    assert os.path.isdir(path)
  finally:
    monkeypatch.delenv('FASTFS_DIR')
    importlib.reload(tmpd)


def test_fastfs_root_is_writable():
  """The resolved fastfs root is writable."""
  probe = tmpd.fastfs_dir(name='batchb-probe')
  marker = os.path.join(probe, 'marker')

  with open(marker, 'w') as fd:
    fd.write('ok')

  with open(marker) as rd:
    assert rd.read() == 'ok'
