import re


def _build_skiprx(qmap):
  stopvals = sorted(qmap.keys())

  return re.compile(r'[\\' + ''.join([rf'\{c}' for c in stopvals]) + ']')


def _split_forward(data, pos, split_rx, quote_rx, seq):
  pdata = data[pos:]

  xm = re.search(split_rx, pdata)
  if xm:
    seq_pos, next_pos = xm.start(), xm.end()
  else:
    seq_pos, next_pos = 0, 0

  km = re.search(quote_rx, pdata)
  if km and km.start() < seq_pos:
    seq_pos = next_pos = km.start()
    xm = None

  if seq_pos:
    seq.extend(tuple(pdata[: seq_pos]))

  return pos + next_pos, xm is not None


class Quote:

  def __init__(self, openc, closec):
    self.openc = openc
    self.closec = closec
    self.count = 1


_QUOTE_MAP = {'"': '"', "'": "'", '(': ')', '{': '}', '[': ']'}
_QUOTE_RX = _build_skiprx(_QUOTE_MAP)

def split(data, split_rx, quote_map=None):
  if quote_map is None:
    quote_map, quote_rx = _QUOTE_MAP, _QUOTE_RX
  else:
    quote_rx = _build_skiprx(quote_map)

  split_rx = re.compile(split_rx) if isinstance(split_rx, str) else split_rx

  pos, qstack, seq, parts = 0, [], [], []
  while pos < len(data):
    c = data[pos]
    if seq and seq[-1] == '\\':
      seq[-1] = c
      pos += 1
    elif not qstack:
      kpos, is_split = _split_forward(data, pos, split_rx, quote_rx, seq)
      if is_split:
        if seq:
          parts.append(''.join(seq))
          seq = ['']
      elif kpos < len(data):
        if kpos - 1 > pos:
          c = data[kpos - 1]
        cc = quote_map.get(c)
        if cc is not None:
          qstack.append(Quote(c if c != cc else None, cc))
        seq.append(c)
      pos = max(kpos, pos + 1)
    else:
      tq = qstack[-1]
      if c == tq.closec:
        tq.count -= 1
        if not tq.count:
          qstack.pop()
      elif c == tq.openc:
        tq.count += 1
      seq.append(c)
      pos += 1

  if seq:
    parts.append(''.join(seq))

  return tuple(parts)


def unquote(data, quote_map=None):
  if len(data) >= 2:
    quote_map = quote_map or _QUOTE_MAP
    cc = quote_map.get(data[0])
    if cc == data[-1]:
      return data[1: -1]

  return data

