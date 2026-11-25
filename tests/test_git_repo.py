"""Tests for py_misc_utils.git_repo.

Skipped when the git binary is not available. Uses a real local repo in
tmp_path so no remote network access is required.
"""

import os
import shutil
import subprocess

import pytest

from py_misc_utils import git_repo as gr


@pytest.fixture
def git_available():
  return shutil.which('git') is not None


@pytest.fixture
def repo(tmp_path, git_available):
  if not git_available:
    pytest.skip('git binary not available')
  repo_dir = os.path.join(tmp_path, 'repo')
  # init a bare-ish local repo with one commit
  os.makedirs(repo_dir)
  subprocess.run(['git', 'init', '-q'], cwd=repo_dir, check=True)
  subprocess.run(['git', 'config', 'user.email', 'test@test'], cwd=repo_dir, check=True)
  subprocess.run(['git', 'config', 'user.name', 'Test'], cwd=repo_dir, check=True)
  readme = os.path.join(repo_dir, 'README.md')
  with open(readme, 'w') as f:
    f.write('# test\n')
  subprocess.run(['git', 'add', 'README.md'], cwd=repo_dir, check=True)
  subprocess.run(['git', 'commit', '-q', '-m', 'initial'], cwd=repo_dir, check=True)
  return repo_dir


def test_git_repo_init(repo, git_available):
  if not git_available:
    pytest.skip('git binary not available')
  gr.GitRepo(repo)  # Should not raise.


def test_git_repo_is_shallow(repo, git_available):
  if not git_available:
    pytest.skip('git binary not available')
  grepo = gr.GitRepo(repo)
  assert grepo.is_shallow() is False


def test_git_repo_checkout(repo, git_available):
  if not git_available:
    pytest.skip('git binary not available')
  grepo = gr.GitRepo(repo)
  grepo.checkout('HEAD')  # Should not raise.
