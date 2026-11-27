"""Random number utilities for seeding, sampling, and random string generation.

This module provides functions to compute deterministic seeds from arbitrary
values, seed numpy/random jointly, weighted random choices, shuffling,
uniform jittering, and cryptographically-random string generation.
"""

import binascii
import random
import string
import struct

import numpy as np

from . import assert_checks as tas


def compute_seed(seed):
  """Compute a deterministic 32-bit CRC seed from an arbitrary value.

  Args:
    seed: An int, float, bytes, str, or other hashable value.

  Returns:
    A 32-bit unsigned integer seed derived via CRC32.

  Example:
    rnd_utils.compute_seed(42) == rnd_utils.compute_seed(42)  # True
    isinstance(rnd_utils.compute_seed('hello'), int)  # True
  """
  if isinstance(seed, int):
    seed = binascii.crc32(struct.pack('=q', seed))
  elif isinstance(seed, float):
    seed = binascii.crc32(struct.pack('=d', seed))
  elif isinstance(seed, bytes):
    seed = binascii.crc32(seed)
  elif isinstance(seed, str):
    seed = binascii.crc32(seed.encode())
  else:
    seed = binascii.crc32(struct.pack('=Q', hash(seed) & 0xFFFFFFFFFFFFFFFF))

  return seed


def manual_seed(seed):
  """Seed both numpy and random with a deterministic seed derived from seed.

  Args:
    seed: An int, float, bytes, str, or other hashable value.

  Returns:
    The computed 32-bit seed actually used.

  Example:
    rnd_utils.manual_seed('abc')  # Seeds np.random and random, returns the seed.
  """
  cseed = compute_seed(seed)

  np.random.seed(cseed)
  random.seed(cseed)

  return cseed


def choices(weights, n):
  """Randomly select n indices weighted by the given weights.

  Args:
    weights: A list of relative weights, one per index.
    n: The number of selections to make.

  Returns:
    A list of n selected indices.

  Example:
    rnd_utils.manual_seed(0)
    rnd_utils.choices([1, 2, 3], 5)  # e.g. [2, 2, 0, 2, 2]
  """
  return random.choices(range(len(weights)), weights=weights, k=n)


def shuffle(args):
  """Return a new shuffled copy of args using random.sample.

  Args:
    args: A sequence of items to shuffle.

  Returns:
    A new list with the same items in random order.

  Example:
    rnd_utils.manual_seed(7)
    rnd_utils.shuffle([1, 2, 3, 4, 5])  # e.g. [3, 5, 2, 1, 4]
  """
  return random.sample(args, k=len(args))


def uniform(center, delta=None, pct=None):
  """Generate a random float uniformly distributed around center.

  Exactly one of delta or pct must be provided.

  Args:
    center: The center value of the distribution.
    delta: Absolute half-width of the distribution.
    pct: Percentage of center used as delta (overrides delta if given).

  Returns:
    A random float in [center - delta, center + delta].

  Raises:
    AssertionError: When neither delta nor pct is provided.

  Example:
    rnd_utils.uniform(10, delta=2)  # A float in [8.0, 12.0].
    rnd_utils.uniform(10, pct=0.1)  # A float in [9.0, 11.0].
  """
  if pct is not None:
    delta = abs(center * pct)

  tas.check_is_not_none(delta, msg='Either delta or pct must be provided')

  return random.uniform(center - delta, center + delta)


def rand_string(n):
  """Generate a cryptographically-random alphanumeric string.

  Args:
    n: The length of the string to generate.

  Returns:
    A string of n lowercase letters and digits.

  Example:
    len(rnd_utils.rand_string(8))  # 8
  """
  rng = random.SystemRandom()

  return ''.join(rng.choices(string.ascii_lowercase + string.digits, k=n))
