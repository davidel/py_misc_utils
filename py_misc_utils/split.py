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
  return re.compile(r'[\\' + ''.join([rf'\{c}' for c in sorted(chars)]) + ']')


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
  return SplitContext(map=quote_map,
                      quote_rx=_chars_regex(quote_map.keys()),
                      quote_sprx=_specials_regex(quote_map))


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

  split_rx = re.compile(split_rx) if isinstance(split_rx, str) else split_rx
  skipper = _Skipper(qctx.quote_rx)

  pos, qstack, parts, seq = 0, [], [], array.array('u')
  while pos < len(data):
    if seq and seq[-1] == '\\':
      seq[-1] = data[pos]
      pos += 1
    elif qstack:
      m = re.search(qctx.quote_sprx, data[pos:])
      if not m:
        break

      seq.extend(data[pos: pos + m.start()])
      pos += m.start()
      c = data[pos]
      tq = qstack[-1]
      if c == tq.closec:
        qstack.pop()
      elif tq.nest_ok and (cc := qctx.map.get(c)):
        qstack.append(_Quote(cc, pos, c != cc))
      seq.append(c)
      pos += 1
    else:
      kpos, is_split = _split_forward(data, pos, split_rx, skipper, seq)
      if is_split:
        parts.append(seq.tounicode())
        seq = array.array('u')
      elif kpos < len(data):
        c = data[kpos]
        if cc := qctx.map.get(c):
          qstack.append(_Quote(cc, kpos, c != cc))
        seq.append(c)
        kpos += 1
      pos = max(kpos, pos + 1)

  tas.check_eq(len(qstack), 0, msg=f'Unmatched quotes during split: "{data}"\n  {qstack}')
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

