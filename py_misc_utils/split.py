import array
import collections
import re

from . import assert_checks as tas


class _Skipper:

  def __init__(self, quote_rx):
    self.quote_rx = quote_rx
    self.next_pos = 0

  def skip(self, data, pos):
    next_pos = self.next_pos - pos
    if next_pos <= 0:
      m = re.search(self.quote_rx, data)
      next_pos = m.start() if m else len(data)
      self.next_pos = pos + next_pos

    return next_pos


def _chars_regex(chars):
  rexs = bytearray(b'[\\')
  for c in sorted(chars):
    rexs.extend((ord('\\'), c))

  rexs.append(ord(']'))

  return re.compile(bytes(rexs))


def _specials_regex(qmap):
  return _chars_regex(set(tuple(qmap.keys()) + tuple(qmap.values())))


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

  seq.extend(pdata[: seq_pos])

  return pos + next_pos, xm is not None


SplitContext = collections.namedtuple('SplitContext', 'map, quote_rx, quote_sprx')

def make_context(quote_map):
  bmap = {ord(k): ord(v) for k, v in quote_map.items()}

  return SplitContext(map=bmap,
                      quote_rx=_chars_regex(bmap.keys()),
                      quote_sprx=_specials_regex(bmap))


def _to_bytes(data, split_rx):
  if isinstance(data, str):
    data = data.encode()
  if isinstance(split_rx, str):
    split_rx = split_rx.encode()

  split_rx = re.compile(split_rx) if isinstance(split_rx, bytes) else split_rx

  return memoryview(data), split_rx


_QUOTE_MAP = {
  '"': '"',
  "'": "'",
  '`': '`',
  '(': ')',
  '{': '}',
  '[': ']',
  '<': '>',
}
_QUOTE_CTX = make_context(_QUOTE_MAP)

_Quote = collections.namedtuple('Quote', 'closec, pos, nest_ok')

def split(data, split_rx, quote_ctx=None):
  qctx = quote_ctx or _QUOTE_CTX

  bdata, bsplit_rx = _to_bytes(data, split_rx)
  skipper = _Skipper(qctx.quote_rx)

  spos, sval = -1, ord('\\')
  pos, qstack, parts, seq = 0, [], [], array.array('B')
  while pos < len(bdata):
    if seq and seq[-1] == sval and pos > spos:
      if (c := bdata[pos]) != sval:
        seq[-1] = c
      pos += 1
      spos = pos
    elif qstack:
      m = re.search(qctx.quote_sprx, bdata[pos:])
      if not m:
        break

      seq.extend(bdata[pos: pos + m.start()])
      pos += m.start()
      c = bdata[pos]
      tq = qstack[-1]
      if c == tq.closec:
        qstack.pop()
      elif tq.nest_ok and (cc := qctx.map.get(c)):
        qstack.append(_Quote(cc, pos, c != cc))
      seq.append(c)
      pos += 1
    else:
      kpos, is_split = _split_forward(bdata, pos, bsplit_rx, skipper, seq)
      if is_split:
        parts.append(seq)
        seq = array.array('B')
      elif kpos < len(bdata):
        c = bdata[kpos]
        if cc := qctx.map.get(c):
          qstack.append(_Quote(cc, kpos, c != cc))
        seq.append(c)
        kpos += 1
      pos = max(kpos, pos + 1)

  tas.check_eq(len(qstack), 0, msg=f'Unmatched quotes during split: "{data}"\n  {qstack}')
  if seq or parts:
    parts.append(seq)

  decode = (lambda b: b.decode()) if isinstance(data, str) else (lambda b: b)

  return tuple(decode(p.tobytes()) for p in parts)


def unquote(data, quote_map=None):
  if len(data) >= 2:
    quote_map = quote_map or _QUOTE_MAP
    cc = quote_map.get(data[0])
    if cc == data[-1]:
      return data[1: -1]

  return data

