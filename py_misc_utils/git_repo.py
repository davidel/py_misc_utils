"""Git repository management utilities.

This module provides the GitRepo class, a wrapper around common git operations
(clone, pull, checkout, etc.) for managing local repository checkouts
programmatically.
"""

import os
import shutil
import subprocess

from . import alog
from . import assert_checks as tas


class GitRepo:
  """Wrapper for git operations on a local repository path.

  Provides methods to clone, pull, checkout, and inspect a git repository
  at a given filesystem path. All git commands are executed via subprocess.

  Attributes:
    path: The filesystem path to the git repository.

  Example:
    import os
    import subprocess
    import tempfile

    from py_misc_utils import git_repo as gr

    # Build a local source repo (stand-in for the remote).
    src = os.path.join(tempfile.mkdtemp(), 'src')
    os.makedirs(src)
    subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=src, check=True)
    subprocess.run(['git', 'config', 'user.email', 't@t'], cwd=src, check=True)
    subprocess.run(['git', 'config', 'user.name', 'T'], cwd=src, check=True)
    readme = os.path.join(src, 'README.md')
    with open(readme, 'w') as f:
      f.write('hello' + chr(10))
    subprocess.run(['git', 'add', 'README.md'], cwd=src, check=True)
    subprocess.run(['git', 'commit', '-q', '-m', 'init'], cwd=src, check=True)

    grepo = gr.GitRepo(os.path.join(os.path.dirname(src), 'dst'))
    grepo.clone(src)
    assert grepo.repo() == src
    assert grepo.current_commit() == subprocess.run(
        ['git', 'rev-parse', 'HEAD'], cwd=src, check=True,
        capture_output=True, text=True).stdout.strip()
    assert grepo.is_shallow() is False
  """

  def __init__(self, path):
    """Initialize a GitRepo for the given path.

    Args:
      path: Filesystem path where the git repository is (or will be) located.
    """
    self.path = path

  def _git(self, *cmd):
    """Construct a git command list for the repository path.

    Args:
      *cmd: Git subcommand and arguments.

    Returns:
      A list suitable for subprocess execution: ['git', '-C', path, *cmd].
    """
    git_cmd = ['git', '-C', self.path] + list(cmd)
    alog.debug(f'Running GIT: {git_cmd}')

    return git_cmd

  def _run(self, *cmd):
    """Execute a git command via subprocess.

    Flattens any list/tuple arguments before passing to subprocess.run.

    Args:
      *cmd: Command arguments (strings or lists/tuples of strings).
    """
    rcmd = []
    for arg in cmd:
      if isinstance(arg, (list, tuple)):
        rcmd.extend(arg)
      else:
        rcmd.appen(arg)

    subprocess.run(rcmd, capture_output=True, check=True)

  def _cmd(self, *cmd):
    """Run a git command without capturing output.

    Args:
      *cmd: Git subcommand and arguments.
    """
    self._run(self._git(*cmd))

  def _outcmd(self, *cmd, strip=False):
    """Run a git command and return its stdout output.

    Args:
      *cmd: Git subcommand and arguments.
      strip: If True, strip whitespace from the output. Defaults to False.

    Returns:
      The command output as a string (or bytes if strip is False and output
      is not decoded).
    """
    output = subprocess.check_output(self._git(*cmd))
    if isinstance(output, bytes):
      output = output.decode()

    return output.strip() if strip else output

  def repo(self):
    """Return the remote origin URL of the repository.

    Returns:
      The remote.origin.url config value as a string.
    """
    return self._outcmd('config', '--get', 'remote.origin.url', strip=True)

  def clone(self, repo, force=False, shallow=False):
    """Clone a repository to self.path if not already present.

    If the path already exists and is a valid repo, verifies the remote matches.
    If force is True or shallow setting differs, purges and re-clones.
    Otherwise pulls changes.

    Args:
      repo: The repository URL to clone.
      force: If True, remove existing directory and re-clone. Defaults to
        False.
      shallow: If True, perform a shallow clone (depth 1). Defaults to
        False.
    """
    do_clone = True
    if os.path.isdir(self.path):
      tas.check_eq(repo, self.repo(), msg='Repo mismatch!')
      if force or shallow != self.is_shallow():
        alog.info(f'Purging old GIT folder: {self.path}')
        shutil.rmtree(self.path)
      else:
        self.pull()
        do_clone = False

    if do_clone:
      parent_path = os.path.dirname(self.path)
      os.makedirs(parent_path, exist_ok=True)
      git_cmd = ['git', '-C', parent_path, 'clone', '-q']
      if shallow:
        git_cmd += ['--depth', '1', repo, os.path.basename(self.path)]
      else:
        git_cmd += [repo, os.path.basename(self.path)]

      alog.debug(f'Running GIT: {git_cmd}')
      self._run(git_cmd)

  def current_commit(self):
    """Return the current HEAD commit hash.

    Returns:
      The SHA1 hash of the current commit as a string.
    """
    return self._outcmd('rev-parse', 'HEAD', strip=True)

  def is_shallow(self):
    """Check if the repository is a shallow clone.

    Returns:
      True if the repository is shallow, False otherwise.
    """
    return self._outcmd('rev-parse', '--is-shallow-repository', strip=True) == 'true'

  def pull(self):
    """Pull changes from the remote repository.

    Runs 'git pull -q' in the repository path.
    """
    self._cmd('pull', '-q')

  def checkout(self, commit):
    """Checkout a specific commit or branch.

    Args:
      commit: The commit hash or branch name to checkout.
    """
    self._cmd('checkout', '-q', commit)
