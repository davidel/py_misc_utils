"""Tests for py_misc_utils.rnd_utils seeding and sampling helpers.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): compute_seed() is deterministic per input type
(int/float/str/bytes) and yields 32-bit values; the generic object
branch masks the hash to 64 bits so any hashable input is accepted;
manual_seed() seeds numpy and random jointly and returns the seed it
used; weighted choices, shuffle copies, and uniform jitter follow the
stdlib random semantics with the documented error paths.
"""

import random

import numpy as np
import pytest

from py_misc_utils import rnd_utils as rnd


def test_compute_seed_deterministic_for_ints():
  """The same int always yields the same 32-bit seed."""
  assert rnd.compute_seed(42) == rnd.compute_seed(42)
  assert 0 <= rnd.compute_seed(42) < 2**32


def test_compute_seed_deterministic_for_strings():
  """The same string always yields the same seed."""
  assert rnd.compute_seed('abc') == rnd.compute_seed('abc')


def test_compute_seed_deterministic_for_floats_and_bytes():
  """Floats and bytes hash via their packed representations."""
  assert rnd.compute_seed(1.5) == rnd.compute_seed(1.5)
  assert rnd.compute_seed(b'ab') == rnd.compute_seed(b'ab')


def test_compute_seed_accepts_hashable_objects():
  """Generic hashables work: the hash is masked to 64 bits, so both
  too-large and negative hashes are accepted."""
  assert rnd.compute_seed((1, 2)) == rnd.compute_seed((1, 2))


class PositiveHash:
  """Object with the largest legal positive hash."""

  def __hash__(self):
    return 2 ** 63 - 1


def test_compute_seed_positive_hash_identity():
  """In-range positive hashes pack without modification."""
  import binascii
  import struct

  expected = binascii.crc32(struct.pack('=Q', 2 ** 63 - 1))
  assert rnd.compute_seed(PositiveHash()) == expected


class NegativeHash:
  """Object with a negative hash, which struct '=Q' cannot pack."""

  def __hash__(self):
    return -7


def test_compute_seed_accepts_negative_hashes():
  """Negative hashes are wrapped modulo 2^64 instead of raising."""
  import binascii
  import struct

  expected = binascii.crc32(struct.pack('=Q', 2 ** 64 - 7))
  assert rnd.compute_seed(NegativeHash()) == expected


def test_manual_seed_returns_computed_seed():
  """manual_seed() returns the seed actually applied."""
  assert rnd.manual_seed('hello') == rnd.compute_seed('hello')


def test_manual_seed_makes_numpy_and_random_reproducible():
  """After manual_seed(), both random engines replay the same stream."""
  rnd.manual_seed(7)
  expected_np = [np.random.random() for _ in range(4)]
  expected_py = [random.random() for _ in range(4)]
  rnd.manual_seed(7)
  assert [np.random.random() for _ in range(4)] == expected_np
  assert [random.random() for _ in range(4)] == expected_py


def test_choices_weights_drive_selection_distribution():
  """A heavily weighted index dominates the drawn indices."""
  draws = rnd.choices([1000, 1, 1], n=200)
  assert all(0 <= idx < 3 for idx in draws)
  assert draws.count(0) > 100


def test_shuffle_returns_new_list_with_same_items():
  """shuffle() returns a permutation of the input as a new list."""
  args = [1, 2, 3, 4]
  shuffled = rnd.shuffle(args)
  assert sorted(shuffled) == args
  assert shuffled is not args


def test_uniform_stays_within_delta():
  """uniform(center, delta) draws within [center - delta, center + delta]."""
  for _ in range(50):
    v = rnd.uniform(10.0, delta=2.0)
    assert 8.0 <= v <= 12.0


def test_uniform_pct_scales_delta_by_center():
  """pct derives the delta from the center (abs for negative centers)."""
  for _ in range(50):
    v = rnd.uniform(-10.0, pct=0.1)
    assert -11.0 <= v <= -9.0


def test_uniform_without_delta_or_pct_raises():
  """One of delta/pct is required (AssertionError otherwise)."""
  with pytest.raises(AssertionError):
    rnd.uniform(10.0)


def test_rand_string_length_and_charset():
  """rand_string() yields n lowercase-alphanumeric characters."""
  s = rnd.rand_string(20)
  assert len(s) == 20
  assert all(c in 'abcdefghijklmnopqrstuvwxyz0123456789' for c in s)
