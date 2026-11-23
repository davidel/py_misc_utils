"""Floating-point bit manipulation utilities.

This module provides functions for converting floating-point values to and
from their IEEE 754 bit representations, with configurable exponent and
mantissa widths.
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
    integer, and mantissa is an integer.
  """
  xm, xe = math.frexp(math.fabs(v))

  e = (xe + exp_bias(nx) - 1) if xm != 0 else 0
  m = int(xm * (1 << (nm + 1)))

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
    return math.inf if m == 0 else math.nan

  xm = float(m | (1 << nm))
  re = e - exp_bias(nx) - nm - 1
  if re >= 0:
    rm = xm * (1 << re)
  else:
    rm = xm / (1 << (-re))

  return rm if s == 0 else -rm
