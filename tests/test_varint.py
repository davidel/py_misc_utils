"""Tests for the py_misc_utils.varint encoder and decoder.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): integers encode with the 7-bit varint scheme,
iterables encode element-wise, decoding yields a list, and invalid
inputs (negative values, truncated buffers) are rejected.
"""

import numpy as np
import pytest

import py_misc_utils.varint as vi


def roundtrip(values):
  """Encode the given values and decode them back."""
  buf = []
  for value in values:
    vi.varint_encode(value, buf)

  return vi.varint_decode(buf)


def test_single_byte_values_encode_to_one_byte():
  """Values below 128 encode to a single low-byte."""
  buf = []
  vi.varint_encode(0, buf)
  vi.varint_encode(1, buf)
  vi.varint_encode(127, buf)

  assert buf == [0, 1, 127]
  assert vi.varint_decode(buf) == [0, 1, 127]


def test_multi_byte_values_use_the_seven_bit_scheme():
  """Values >= 128 set the continuation bit on each partial byte."""
  buf = []
  vi.varint_encode(128, buf)

  assert buf == [0x80, 0x01]
  assert roundtrip([128]) == [128]
  assert roundtrip([300]) == [300]
  assert roundtrip([2**31]) == [2**31]
  assert roundtrip([2**63 - 1]) == [2**63 - 1]


def test_very_large_values_roundtrip():
  """Values up to 2**63 encode and decode correctly."""
  assert roundtrip([2**63]) == [2**63]


def test_iterables_encode_element_wise():
  """Iterables encode each element into the buffer in order."""
  buf = []
  vi.varint_encode([1, 128, 300], buf)
  vi.varint_encode((5,), buf)

  assert vi.varint_decode(buf) == [1, 128, 300, 5]


def test_numpy_signed_integers_roundtrip():
  """numpy signed integer scalars encode and decode like plain ints."""
  buf = []
  vi.varint_encode(np.int64(300), buf)
  vi.varint_encode(np.int32(7), buf)

  assert vi.varint_decode(buf) == [300, 7]


def test_numpy_unsigned_integers_roundtrip():
  """numpy unsigned integer scalars encode and decode like plain ints."""
  buf = []
  vi.varint_encode(np.uint8(42), buf)
  vi.varint_encode(np.uint64(2 ** 40), buf)

  assert vi.varint_decode(buf) == [42, 2 ** 40]


def test_decode_returns_list_of_decoded_values():
  """varint_decode() returns a list regardless of how values got encoded."""
  buf = []
  vi.varint_encode([10, 20], buf)

  result = vi.varint_decode(buf)

  assert result == [10, 20]
  assert type(result) is list


def test_encode_rejects_negative_values():
  """Negative integers are rejected with an AssertionError."""
  with pytest.raises(AssertionError):
    vi.varint_encode(-1, [])


def test_decode_truncated_buffer_raises_value_error():
  """A truncated continuation sequence raises a ValueError."""
  with pytest.raises(ValueError):
    vi.varint_decode([0x80])
  with pytest.raises(ValueError):
    vi.varint_decode([0xff])
