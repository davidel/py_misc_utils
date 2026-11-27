"""Number-theoretic utilities.

This module provides functions for prime factorization, divisor search,
sign extension, bit operations, rounding, linear interpolation, and
perfect-power detection.
"""

import math


def prime_factors(n):
  """Yield the prime factors of n.

  Args:
    n: A positive integer to factorize. Zero yields nothing.

  Yields:
    Prime factors of n in ascending order, with multiplicity.

  Example:
    list(num_utils.prime_factors(360))  # [2, 2, 2, 3, 3, 5]
    list(num_utils.prime_factors(13))  # [13]
  """
  i = 2
  while i * i <= n:
    q, r = divmod(n, i)
    if r == 0:
      n = q
      yield i
    else:
      i += 1

  if n > 1:
    yield n


def nearest_divisor(value, n):
  """Find the divisor of a value closest to a target number.

  Args:
    value: The value whose divisors are searched.
    n: The target number the divisor should be as close as possible to.

  Returns:
    The divisor of value nearest to n - ties resolved towards the lower
    divisor.

  Example:
    num_utils.nearest_divisor(100, 6)  # 5: the divisor closest to 6.
    num_utils.nearest_divisor(24, 5)  # 4: tie (4 vs 6) resolved downwards.
  """
  nmin, nmax = n, n

  while nmin > 1:
    if value % nmin == 0:
      break
    nmin -= 1

  dmin = n - nmin
  top_n = min(n + dmin, value // 2)

  while nmax <= top_n:
    if value % nmax == 0:
      break
    nmax += 1

  if value % nmax != 0:
    return nmin

  return nmin if dmin <= (nmax - n) else nmax


def sign_extend(value, nbits):
  """Sign-extend an nbits-wide two's complement value to Python int.

  Args:
    value: The value to extend.
    nbits: The bit width of the value.

  Returns:
    The negative value when the sign bit is set, the value otherwise.

  Example:
    num_utils.sign_extend(0xFF, 8)  # -1
    num_utils.sign_extend(0x7F, 8)  # 127
  """
  sign = 1 << (nbits - 1)

  return (value & (sign - 1)) - (value & sign)


def ffs(n):
  """Find the index of the least significant set bit (find-first-set).

  Args:
    n: A non-zero integer.

  Returns:
    The 0-based index of the lowest set bit of n.

  Example:
    num_utils.ffs(8)  # 3
    num_utils.ffs(12)  # 2
  """
  nnorm = n & (-n)

  return nnorm.bit_length() - 1


def round_up(v, step):
  """Round a value up to the next multiple of a step.

  Args:
    v: The value to round.
    step: The rounding step.

  Returns:
    The smallest multiple of step which is greater than or equal to v.

  Example:
    num_utils.round_up(17, 5)  # 20
    num_utils.round_down(17, 5)  # 15
  """
  return ((v + step - 1) // step) * step


def round_down(v, step):
  """Round a value down to the previous multiple of a step.

  Args:
    v: The value to round.
    step: The rounding step.

  Returns:
    The largest multiple of step which is less than or equal to v.
  """
  return (v // step) * step


def mix(a, b, gamma):
  """Linearly interpolate two values.

  Args:
    a: The value returned when gamma is 1.0.
    b: The value returned when gamma is 0.0.
    gamma: The interpolation factor.

  Returns:
    a * gamma + b * (1.0 - gamma).

  Example:
    num_utils.mix(10, 20, 0.25)  # 17.5
  """
  return a * gamma + b * (1.0 - gamma)


def perfect_power(value, base):
  """Return the exponent when value is a power of base.

  Args:
    value: The value to check.
    base: The base of the power.

  Returns:
    The exponent n when value == base**n, or None otherwise.

  Example:
    num_utils.perfect_power(8, 2)  # 3
    num_utils.perfect_power(10, 2)  # None
  """
  vlog = round(math.log(value, base))

  return vlog if base**vlog == value else None


def address_bits(size):
  """Return the number of bits needed to address a memory size.

  Args:
    size: The number of addressable units.

  Returns:
    The number of address bits: 1 for sizes <= 1, the bit length of
    size - 1 otherwise.
  """
  return (size - 1).bit_length() if size > 1 else 1
