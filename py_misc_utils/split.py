import array
import collections
import re

from . import assert_checks as tas


_Quote = collections.namedtuple('Quote', 'closec, nest_ok')


class _Skipper:

  def __init__(self, quote_rx):
    self.quote_rx = quote_rx
    self.next_pos = 0

  def skip(self, data, pos):
    next_pos = self.next_pos - pos
    if next_pos <= 0:
      m = re.search(self.quote_rx, data)
      if m:
        self.next_pos, next_pos = pos + m.start(), m.start()
      else:
        self.next_pos, next_pos = pos + len(data), len(data)

    return next_pos


def _build_skiprx(qmap):
  stopvals = sorted(qmap.keys())

  return re.compile(r'[\\' + ''.join([rf'\{c}' for c in stopvals]) + ']')


def _split_forward(data, pos, split_rx, skipper, seq):
  pdata = data[pos:]

  xm = re.search(split_rx, pdata)
  if xm:
    seq_pos, next_pos = xm.start(), xm.end()
  else:
    seq_pos = next_pos = len(pdata)

  skip_pos = skipper.skip(pdata, pos)
  if skip_pos < seq_pos:
    seq_pos = next_pos = skip_pos
    xm = None

  if seq_pos:
    seq.extend(pdata[: seq_pos])

  return pos + next_pos, xm is not None


_QUOTE_MAP = {'"': '"', "'": "'", '(': ')', '{': '}', '[': ']', '<': '>'}
_QUOTE_RX = _build_skiprx(_QUOTE_MAP)

def split(data, split_rx, quote_map=None):
  if quote_map is None:
    quote_map, quote_rx = _QUOTE_MAP, _QUOTE_RX
  else:
    quote_rx = _build_skiprx(quote_map)

  split_rx = re.compile(split_rx) if isinstance(split_rx, str) else split_rx
  skipper = _Skipper(quote_rx)

  pos, qstack, parts, seq = 0, [], [], array.array('u')
  while pos < len(data):
    c = data[pos]
    if seq and seq[-1] == '\\':
      seq[-1] = c
      pos += 1
    elif not qstack:
      kpos, is_split = _split_forward(data, pos, split_rx, skipper, seq)
      if is_split:
        if seq or parts:
          parts.append(seq.tounicode())
          seq = array.array('u')
      elif kpos < len(data):
        c = data[kpos]
        if cc := quote_map.get(c):
          qstack.append(_Quote(cc, c != cc))
        seq.append(c)
        kpos += 1
      pos = max(kpos, pos + 1)
    else:
      tq = qstack[-1]
      if c == tq.closec:
        qstack.pop()
      elif tq.nest_ok and (cc := quote_map.get(c)):
        qstack.append(_Quote(cc, c != cc))
      seq.append(c)
      pos += 1

  tas.check_eq(len(qstack), 0, msg=f'Unmatched quotes during split: {qstack}')
  if seq or parts:
    parts.append(seq.tounicode())

  return tuple(parts)


def unquote(data, quote_map=None):
  if len(data) >= 2:
    quote_map = quote_map or _QUOTE_MAP
    cc = quote_map.get(data[0])
    if cc == data[-1]:
      return data[1: -1]

  return data

