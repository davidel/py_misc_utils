import numpy as np

from . import alog
from . import assert_checks as tas


def varint_encode(v, encbuf):
  if isinstance(v, (int, np.integer)):
    tas.check_ge(v, 0, msg=f'Cannot encode negative values: {v}')

    cv = v
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
  values, cpos = [], 0
  try:
    while cpos < len(encbuf):
      value, cpos = _varint_decode(encbuf, cpos)
      values.append(value)
  except IndexError:
    # Handle out-of-range buffer access outside, to avoid checks within the inner loop.
    enc_data = ','.join(f'0x{x:02x}' for x in encbuf[cpos: cpos + 10])
    alog.xraise(ValueError,
                f'Invalid varint encoded buffer content at offset {cpos}: {enc_data} ...')

  return values

