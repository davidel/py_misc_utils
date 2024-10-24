import sys

import fsspec

from . import context_managers as cm


_STD_FILES = {
  'STDIN': sys.stdin,
  'STDOUT': sys.stdout,
  'STDERR': sys.stderr,
}

def gen_open(path, *args, **kwargs):
  sfd = _STD_FILES.get(path)
  if sfd is not None:
    return cm.NoOpCtxManager(sfd)

  return fsspec.open(path, *args, **kwargs)

