import binascii
import random
import string
import struct

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


def shuffle(args):
  return random.sample(args, k=len(args))


def uniform(center, delta=None, pct=None):
  if pct is not None:
    delta = abs(center * pct)

  tas.check_is_not_none(delta, msg=f'Either delta or pct must be provided')

  return random.uniform(center - delta, center + delta)


def rand_string(n):
  rng = random.SystemRandom()

  return ''.join(rng.choices(string.ascii_lowercase + string.digits, k=n))

