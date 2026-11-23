"""Floating-point bit manipulation utilities.

This module provides functions for converting floating-point values to and
from sign/exponent/mantissa bit representations with configurable exponent
and mantissa widths, following the IEEE 754 binary interchange conventions:
exponent bias of 2^(nx-1) - 1, subnormals for zero exponents, and
all-ones exponents reserved for inf/NaN.

Rounding on encode truncates towards zero (not IEEE round-to-nearest), so
values requiring more than nm mantissa bits lose their trailing bits.
"""

import math


def exp_bias(nx):
  """Compute the exponent bias for a given exponent width.

  The bias is (2^(nx-1)) - 1, used to offset the exponent in floating-point
  representation.

  Args:
    nx: Number of exponent bits.

  Returns:
    The exponent bias integer.
  """
  return (1 << (nx - 1)) - 1


def real_to_bits(v, nx, nm):
  """Convert a real value to sign, exponent, and mantissa bit components.

  Args:
    v: The floating-point value to convert.
    nx: Number of exponent bits.
    nm: Number of mantissa bits.

  Returns:
    A tuple (sign, exponent, mantissa) where sign is 0 or 1, exponent is an
    integer, and mantissa is an integer. Negative zero yields sign 1.
    Values below the subnormal range flush to zero. Non-finite inputs raise
    ValueError.

  Raises:
    ValueError: If v is not a finite number.
  """
  if not math.isfinite(v):
    raise ValueError(f'Cannot encode non-finite value: {v}')

  # Preserving the sign of negative zero: v < 0 is False for -0.0.
  if v == 0.0:
    return 1 if math.copysign(1.0, v) < 0 else 0, 0, 0

  xm, xe = math.frexp(math.fabs(v))
  bias = exp_bias(nx)

  e = xe + bias - 1
  if e <= 0:
    # Subnormal: keep the leading bits of the mantissa with the fixed
    # minimum exponent, or flush to zero below the subnormal range.
    shift = nm + e
    if shift <= 0:
      return 1 if v < 0 else 0, 0, 0

    return 1 if v < 0 else 0, 0, int(xm * (1 << shift))

  m = int(xm * (1 << nm))
  if e >= (1 << nx):
    # Overflow: saturate to infinity with the sign of v.
    return 1 if v < 0 else 0, (1 << nx) - 1, 0

  return 1 if v < 0 else 0, e, m


def pack_bits(s, e, m, nx, nm):
  """Pack sign, exponent, and mantissa into a single integer.

  Args:
    s: Sign bit (0 or 1).
    e: Exponent value.
    m: Mantissa value.
    nx: Number of exponent bits.
    nm: Number of mantissa bits.

  Returns:
    The packed integer representation.
  """
  return (s << (nx + nm)) | (e << nm) | m


def real_to_packedbits(v, nx, nm):
  """Convert a real value to its packed bit representation.

  Args:
    v: The floating-point value to convert.
    nx: Number of exponent bits.
    nm: Number of mantissa bits.

  Returns:
    The packed integer bit representation.
  """
  s, e, m = real_to_bits(v, nx, nm)

  return pack_bits(s, e, m, nx, nm)


def _bits(v, pos, n):
  """Extract n bits from v starting at position pos.

  Args:
    v: The integer value to extract bits from.
    pos: The bit position to start extracting from.
    n: The number of bits to extract.

  Returns:
    The extracted n-bit integer value.
  """
  return (v >> pos) & ((1 << n) - 1)


def packedbits_to_real(v, nx, nm):
  """Convert a packed bit integer back to a real floating-point value.

  Args:
    v: The packed integer bit representation.
    nx: Number of exponent bits.
    nm: Number of mantissa bits.

  Returns:
    The decoded floating-point value, or math.inf/math.nan for special cases.
  """
  s, e, m = _bits(v, nx + nm, 1), _bits(v, nm, nx), _bits(v, 0, nm)

  if e == 0 and m == 0:
    return 0.0
  if e == ((1 << nx) - 1):
    if m == 0:
      return math.inf if s == 0 else -math.inf

    return math.nan

  # The mantissa keeps its leading bit in bit nm - 1, so the value is
  # m * 2^(e - bias - nm + 1); for a zero exponent this is the IEEE
  # subnormal formula m * 2^(1 - bias - nm).
  re = e - exp_bias(nx) - nm + 1
  if re >= 0:
    rm = m * (1 << re)
  else:
    rm = m / (1 << (-re))

  return rm if s == 0 else -rm
