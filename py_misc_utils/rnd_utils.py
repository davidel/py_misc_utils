import binascii
import os
import random
import string
import struct
import tempfile

import numpy as np

from . import assert_checks as tas


def compute_seed(seed):
  if isinstance(seed, int):
    seed = binascii.crc32(struct.pack('=q', seed))
  elif isinstance(seed, float):
    seed = binascii.crc32(struct.pack('=d', seed))
  elif isinstance(seed, bytes):
    seed = binascii.crc32(seed)
  elif isinstance(seed, str):
    seed = binascii.crc32(seed.encode())
  else:
    seed = binascii.crc32(struct.pack('=Q', hash(seed)))

  return seed


def manual_seed(seed):
  cseed = compute_seed(seed)

  np.random.seed(cseed)
  random.seed(cseed)

  return cseed


def choices(weights, n):
  return random.choices(range(len(weights)), weights=weights, k=n)


def uniform(center, delta=None, pct=None):
  if pct is not None:
    delta = center * pct

  tas.check_is_not_none(delta, msg=f'Either delta or pct must be provided')

  return random.uniform(center - delta, center + delta)


def rand_string(n):
  rng = random.SystemRandom()

  return ''.join(rng.choices(string.ascii_lowercase + string.digits, k=n))


_TMPFN_RNDSIZE = int(os.getenv('TMPFN_RNDSIZE', 10))

def temp_path(nspath=None, nsdir=None, rndsize=None):
  rndsize = rndsize or _TMPFN_RNDSIZE

  if nspath is not None:
    return f'{nspath}.{rand_string(rndsize)}'

  nsdir = tempfile.gettempdir() if nsdir is None else nsdir

  return os.path.join(nsdir, f'{rand_string(rndsize)}.tmp')

