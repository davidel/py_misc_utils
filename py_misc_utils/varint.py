"""Varint encoding and decoding of integer values.

This module provides functions to encode non-negative integers -- or
iterables of them -- using the variable-length varint representation,
and to decode such buffers back into lists of integers.
"""

import numpy as np

from . import alog
from . import assert_checks as tas


def varint_encode(v, encbuf):
  """Append the varint encoding of a value to a buffer.

  Integers are encoded using the 7-bit varint scheme; iterables are
  encoded element by element into the same buffer.

  Args:
    v: Non-negative integer, numpy integer or iterable of integers to
      encode.
    encbuf: List-like buffer to append encoded bytes to.
  """
  if isinstance(v, (int, np.integer)):
    tas.check_ge(v, 0, msg=f'Cannot encode negative values: {v}')

    # Unsigned numpy dtypes overflow when masked with the signed ~0x7f
    # pattern, so work on a plain Python integer throughout.
    cv = int(v)
    while (cv & ~0x7f) != 0:
      encbuf.append(0x80 | (cv & 0x7f))
      cv >>= 7

    encbuf.append(cv & 0x7f)
  elif hasattr(v, '__iter__'):
    for xv in v:
      varint_encode(xv, encbuf)
  else:
    alog.xraise(RuntimeError, f'Unsupported type: {v} ({type(v)})')


def _varint_decode(encbuf, pos):
  """Decode a single varint value from a buffer.

  Args:
    encbuf: Buffer holding the encoded bytes.
    pos: Offset in the buffer at which to start decoding.

  Returns:
    The (value, next offset) tuple decoded from the buffer.
  """
  value, cpos, nbits = 0, pos, 0
  while True:
    b = encbuf[cpos]
    value |= (b & 0x7f) << nbits
    nbits += 7
    cpos += 1
    if (b & 0x80) == 0:
      break

  return value, cpos


def varint_decode(encbuf):
  """Decode all the varint values contained in a buffer.

  Args:
    encbuf: Buffer holding the encoded bytes.

  Returns:
    A list with all the values decoded from the buffer.

  Raises:
    ValueError: If the buffer does not contain a valid varint stream.
  """
  values, cpos = [], 0
  try:
    while cpos < len(encbuf):
      value, cpos = _varint_decode(encbuf, cpos)
      values.append(value)
  except IndexError:
    # Handle out-of-range buffer access outside, to avoid checks within the inner loop.
    enc_data = ','.join(f'0x{x:02x}' for x in encbuf[cpos:cpos + 10])
    alog.xraise(
        ValueError,
        f'Invalid varint encoded buffer content at offset {cpos}: {enc_data} ...')

  return values
