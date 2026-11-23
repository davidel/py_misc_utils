"""Tests for py_misc_utils.fp_utils custom floating-point bit coding.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): values follow IEEE 754 binary interchange
conventions with an explicit leading mantissa bit. Encode truncates
towards zero (documented deviation), saturates overflows to the sign'd
infinity code, supports subnormals and underflow-to-zero, raises
ValueError for non-finite inputs, and decode resolves all reserved
exponent codes (zero, subnormal, inf, nan).
"""

import math

import pytest

import py_misc_utils.fp_utils as fpu


def pack(s, e, m, nx, nm):
  """Pack sign/exponent/mantissa bits into a single integer."""
  return (s << (nx + nm)) | (e << nm) | m


def roundtrip(v, nx, nm):
  """Encode and decode a value, returning the decoded result."""
  s, e, m = fpu.real_to_bits(v, nx, nm)
  packed = fpu.pack_bits(s, e, m, nx, nm)

  return fpu.packedbits_to_real(packed, nx, nm)


def test_exp_bias_is_2_pow_nx_minus_1():
  """The bias follows the IEEE 754 convention."""
  assert fpu.exp_bias(8) == 127
  assert fpu.exp_bias(4) == 7
  assert fpu.exp_bias(11) == 1023


def test_packing_bit_layout_matches_sign_exp_mantissa():
  """pack_bits() places sign highest, then exponent, then mantissa."""
  assert fpu.pack_bits(1, 3, 5, 4, 3) == 0b1_0011_101


def test_real_to_bits_splits_sign_exponent_mantissa():
  """1.5 encodes as sign 0, unbiased-exponent-plus-bias, mantissa with
  the leading bit set in bit nm - 1."""
  s, e, m = fpu.real_to_bits(1.5, 8, 23)
  assert (s, e, m) == (0, 127, 0x600000)
  s, e, m = fpu.real_to_bits(-1.5, 8, 23)
  assert (s, e, m) == (1, 127, 0x600000)


def test_normals_roundtrip_exactly():
  """Values representable in the format decode back to themselves."""
  for v in (1.5, -1.5, 0.25, -0.25, 100.0, 7.0, 123.0, -64.0):
    assert roundtrip(v, 8, 23) == v


def test_zero_encodes_to_all_zero_bits():
  """Zero is the all-zero code."""
  s, e, m = fpu.real_to_bits(0.0, 8, 23)
  assert (s, e, m) == (0, 0, 0)


def test_negative_zero_keeps_sign_bit():
  """-0.0 encodes with sign bit 1 and decodes to a zero value."""
  s, e, m = fpu.real_to_bits(-0.0, 8, 23)
  assert s == 1
  assert fpu.packedbits_to_real(pack(s, e, m, 8, 23), 8, 23) == 0.0


def test_non_finite_encode_raises_value_error():
  """Encoding inf/nan raises ValueError (documented deviation)."""
  with pytest.raises(ValueError):
    fpu.real_to_bits(math.inf, 8, 23)
  with pytest.raises(ValueError):
    fpu.real_to_bits(-math.inf, 8, 23)
  with pytest.raises(ValueError):
    fpu.real_to_bits(math.nan, 8, 23)


def test_encode_saturates_overflow_to_infinity():
  """Values beyond the exponent range saturate to the inf code."""
  s, e, m = fpu.real_to_bits(1e30, 4, 3)
  assert (s, e, m) == (0, 15, 0)
  s, e, m = fpu.real_to_bits(-1e30, 4, 3)
  assert (s, e, m) == (1, 15, 0)


def test_encode_flushes_underflow_to_zero():
  """Values below the subnormal range encode to the all-zero code."""
  assert fpu.real_to_bits(1e-50, 8, 23) == (0, 0, 0)


def test_subnormal_encode_decode():
  """Small values encode to subnormals and decode to their value."""
  # Smallest single-precision subnormal: 2^-149.
  p = fpu.real_to_packedbits(2.0**-149, 8, 23)
  assert p == 1
  assert fpu.packedbits_to_real(1, 8, 23) == 2.0**-149


def test_decode_reserved_codes():
  """All-ones exponent resolves to inf/nan; zero exponent to
  subnormals."""
  assert fpu.packedbits_to_real(pack(0, 255, 0, 8, 23), 8, 23) == math.inf
  assert fpu.packedbits_to_real(pack(1, 255, 0, 8, 23), 8, 23) == -math.inf
  assert math.isnan(fpu.packedbits_to_real(pack(1, 255, 5, 8, 23), 8, 23))
  # Smallest subnormal code.
  assert fpu.packedbits_to_real(1, 8, 23) == 2.0**-149


def test_low_precision_truncates_towards_zero():
  """Truncation on encode is the documented deviation from IEEE 754."""
  assert roundtrip(3.75, 4, 3) == 3.5
  assert roundtrip(123.456, 8, 23) == 123.45599365234375


def test_packedbits_to_real_rejects_reserved_nan():
  """A nan code decodes to nan, not a numeric value."""
  assert math.isnan(fpu.packedbits_to_real(pack(0, 15, 3, 4, 3), 4, 3))
