import os
import shutil
import subprocess

from . import alog
from . import assert_checks as tas


class GitRepo:

  def __init__(self, path):
    self.path = path

  def _git(self, *cmd):
    git_cmd = ['git', '-C', self.path] + list(cmd)
    alog.debug(f'Running GIT: {git_cmd}')

    return git_cmd

  def _run(self, *cmd):
    subprocess.run(cmd, capture_output=True, check=True)

  def _cmd(self, *cmd):
    self._run(*self._git(*cmd))

  def _outcmd(self, *cmd, strip=False):
    output = subprocess.check_output(self._git(*cmd))
    if isinstance(output, bytes):
      output = output.decode()

    return output.strip() if strip else output

  def repo(self):
    return self._outcmd('config', '--get', 'remote.origin.url', strip=True)

  def clone(self, repo, force=False, shallow=False):
    do_clone = True
    if os.path.isdir(self.path):
      tas.check_eq(repo, self.repo(), msg=f'Repo mismatch!')
      if force or shallow != self.is_shallow():
        alog.info(f'Purging old GIT folder: {self.path}')
        shutil.rmtree(self.path)
      else:
        self.pull()
        do_clone = False

    if do_clone:
      parent_path = os.path.dirname(self.path)
      os.makedirs(parent_path, exist_ok=True)
      if shallow:
        git_cmd = ['git', '-C', parent_path, 'clone', '-q', '--depth', '1',
                   repo, os.path.basename(self.path)]
      else:
        git_cmd = ['git', '-C', parent_path, 'clone', '-q', repo, os.path.basename(self.path)]

      alog.debug(f'Running GIT: {git_cmd}')
      self._run(*git_cmd)

  def current_commit(self):
    return self._outcmd('rev-parse', 'HEAD', strip=True)

  def is_shallow(self):
    return self._outcmd('rev-parse', '--is-shallow-repository', strip=True) == 'true'

  def pull(self):
    self._cmd('pull', '-q')

  def checkout(self, commit):
    self._cmd('checkout', '-q', commit)

