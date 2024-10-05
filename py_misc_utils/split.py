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


_QUOTE_MAP = {'"': '"', "'": "'", '(': ')', '{': '}', '[': ']'}
_QUOTE_RX = _build_skiprx(_QUOTE_MAP)

def split(data, split_rx, quote_map=None):
  if quote_map is None:
    quote_map, quote_rx = _QUOTE_MAP, _QUOTE_RX
  else:
    quote_rx = _build_skiprx(quote_map)

  split_rx = re.compile(split_rx) if isinstance(split_rx, str) else split_rx

  seq, parts = [], []
  pos, oc, cc, count = 0, None, None, 0
  while pos < len(data):
    c = data[pos]
    if seq and seq[-1] == '\\':
      seq[-1] = c
      pos += 1
    elif count == 0:
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
          oc = c if c != cc else None
          count += 1
        seq.append(c)
      pos = max(kpos, pos + 1)
    else:
      if c == cc:
        count -= 1
      elif c == oc:
        count += 1
      seq.append(c)
      pos += 1

  if seq:
    parts.append(''.join(seq))

  return tuple(parts)

