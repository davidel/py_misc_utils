"""Tests for the py_misc_utils.num_utils number-theoretic helpers.

Expectations follow the documented contracts: prime factorization yields
ascending factors, ffs() returns the index of the lowest set bit, rounding
snaps to multiples, mix() interpolates with gamma as the weight of `a`,
and address_bits() reports the bits needed to index `size - 1` units.
"""

import math

import py_misc_utils.num_utils as nu


def test_prime_factors_yields_ascending_factors():
  """prime_factors() yields factors in ascending order, with multiplicity."""
  assert list(nu.prime_factors(12)) == [2, 2, 3]
  assert list(nu.prime_factors(97)) == [97]
  assert list(nu.prime_factors(1)) == []


def test_prime_factors_product_reconstructs_the_input():
  """Multiplying the yielded factors reproduces the original number."""
  for n in (2, 30, 128, 1001, 999983):
    product = math.prod(nu.prime_factors(n))

    assert product == n


def test_ffs_returns_lowest_set_bit_index():
  """ffs() returns the 0-based index of the least significant set bit."""
  assert nu.ffs(1) == 0
  assert nu.ffs(8) == 3
  assert nu.ffs(12) == 2
  assert nu.ffs(1 << 40) == 40


def test_round_up_rounds_to_next_multiple():
  """round_up() moves values up to the next multiple of the grain."""
  assert nu.round_up(13, 5) == 15
  assert nu.round_up(10, 5) == 10
  assert nu.round_up(1, 8) == 8


def test_round_down_rounds_to_previous_multiple():
  """round_down() moves values down to the previous multiple of the grain."""
  assert nu.round_down(13, 5) == 10
  assert nu.round_down(15, 5) == 15


def test_sign_extend_interprets_high_bit_as_sign():
  """sign_extend() maps values with the top bit set to negative numbers."""
  assert nu.sign_extend(5, 8) == 5
  assert nu.sign_extend(0x7F, 8) == 127
  assert nu.sign_extend(0xF0, 8) == -16


def test_nearest_divisor_picks_the_closest_working_divisor():
  """nearest_divisor() returns the largest divisor of size below target."""
  assert nu.nearest_divisor(10, 4) == 5
  assert nu.nearest_divisor(15, 4) == 5
  assert nu.nearest_divisor(7, 3) == 1


def test_mix_interpolates_linearly():
  """mix() weights `a` by gamma and `b` by 1 - gamma."""
  assert nu.mix(0, 10, 0.5) == 5.0
  assert nu.mix(0, 10, 0.0) == 10.0
  assert nu.mix(0, 10, 1.0) == 0.0
  assert nu.mix(0, 10, 0.25) == 7.5


def test_perfect_power_returns_exact_exponent_or_none():
  """perfect_power() yields n when value == base**n, else None."""
  assert nu.perfect_power(8, 2) == 3
  assert nu.perfect_power(16, 2) == 4
  assert nu.perfect_power(27, 3) == 3
  assert nu.perfect_power(125, 5) == 3
  assert nu.perfect_power(1, 2) == 0
  assert nu.perfect_power(9, 2) is None
  # 2 == 2 ** 1, so the exponent is 1.
  assert nu.perfect_power(2, 2) == 1


def test_address_bits_covers_the_addressable_range():
  """address_bits() returns the bits needed to address size - 1 units."""
  assert nu.address_bits(1) == 1
  assert nu.address_bits(2) == 1
  assert nu.address_bits(3) == 2
  assert nu.address_bits(4) == 2
  assert nu.address_bits(5) == 3
  assert nu.address_bits(1024) == 10


def test_prime_factors_yields_nothing_for_zero():
  """prime_factors(0) yields nothing (docstring says positive input only)."""
  assert list(nu.prime_factors(0)) == []
