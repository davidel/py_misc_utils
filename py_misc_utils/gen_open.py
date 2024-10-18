import sys

from . import utils as ut


_STD_FILES = {
  'STDIN': sys.stdin,
  'STDOUT': sys.stdout,
  'STDERR': sys.stderr,
}

def gen_open(path, *args, **kwargs):
  sfd = _STD_FILES.get(path)

  return ut.NoOpCtxManager(sfd) if sfd is not None else open(path, *args, **kwargs)

