"""Quote-aware string splitting utilities.

This module provides functions for splitting strings while respecting quoted
sections and escaped characters. It supports custom quote maps, nesting rules,
and unescaping, making it suitable for parsing shell-like or configuration
syntax.

Classes:
  _Skipper: Tracks quote positions during forward scanning.
  SplitContext: Named tuple holding quote mapping and regex patterns.

Main APIs:
  _chars_regex, _specials_regex, _split_forward, make_context, _to_bytes,
  split, unquote
"""

import collections
import re

from . import assert_checks as tas


class _Skipper:
  """Tracks quote positions during forward scanning.

  The skipper advances through data, identifying where quoted sections begin
  so that split operations do not break on delimiters inside quotes.
  """

  def __init__(self, quote_rx):
    """Initialize the skipper with the quote matcher.

    Args:
      quote_rx: Regular expression matching a quoted section.
    """
    self.quote_rx = quote_rx
    self.next_pos = 0

  def skip(self, data, pos):
    """Advances to the next quote boundary from the given position.

    Args:
      data: The byte string to scan.
      pos: The current position within data.

    Returns:
      The position of the next quote boundary, or end of data if none found.
    """
    next_pos = self.next_pos - pos
    if next_pos <= 0:
      m = re.search(self.quote_rx, data)
      next_pos = m.start() if m else len(data)
      self.next_pos = pos + next_pos

    return next_pos


def _chars_regex(chars):
  """Builds a regex pattern matching any of the given characters.

  Args:
    chars: An iterable of characters to match.

  Returns:
    A compiled regex pattern matching any single character in ``chars``.
  """
  rexs = bytearray(b'[\\')
  for c in sorted(chars):
    rexs.extend((ord('\\'), c))

  rexs.append(ord(']'))

  return re.compile(bytes(rexs))


def _specials_regex(qmap):
  """Builds a regex pattern for all special characters in a quote map.

  Args:
    qmap: A dictionary mapping opening quote characters to closing ones.

  Returns:
    A compiled regex pattern matching any quote opener or closer.
  """
  return _chars_regex(set(tuple(qmap.keys()) + tuple(qmap.values())))


def _split_forward(data, pos, split_rx, skipper, seq):
  """Scans forward from pos, finding the next split point or quote boundary.

  Args:
    data: The byte string to scan.
    pos: The starting position.
    split_rx: A compiled regex pattern for split delimiters.
    skipper: A _Skipper instance tracking quote boundaries.
    seq: A bytearray to accumulate matched content into.

  Returns:
    A tuple ``(new_pos, is_split)`` where ``new_pos`` is the position after
    the split point or quote boundary, and ``is_split`` is True if a split
    delimiter was found.
  """
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

  seq.extend(pdata[:seq_pos])

  return pos + next_pos, xm is not None


SplitContext = collections.namedtuple('SplitContext', 'map, quote_rx, quote_sprx')


def make_context(quote_map):
  """Creates a SplitContext from a quote mapping dictionary.

  Args:
    quote_map: A dict mapping opening quote characters to closing characters.

  Returns:
    A ``SplitContext`` namedtuple with compiled regex patterns for quote
    detection.
  """
  bmap = {ord(k): ord(v) for k, v in quote_map.items()}

  return SplitContext(map=bmap,
                      quote_rx=_chars_regex(bmap.keys()),
                      quote_sprx=_specials_regex(bmap))


def _to_bytes(data, split_rx):
  """Converts data and split_rx to bytes/memoryview for processing.

  Args:
    data: A string or bytes object to split.
    split_rx: A string or compiled regex pattern for the split delimiter.

  Returns:
    A tuple ``(memoryview, compiled_regex)`` suitable for byte-level scanning.
  """
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


def split(data, split_rx, quote_ctx=None, unescape=False):
  """Splits data while respecting quoted sections and escaped characters.

  Args:
    data: A string or bytes object to split.
    split_rx: A string or compiled regex pattern for the split delimiter.
    quote_ctx: An optional SplitContext defining quote characters. Defaults
      to a context with common quote pairs (\"'\", \"'\", \"`\", \"()\", \"{}\",
      \"[]\", \"<>\").
    unescape: If True, backslash-escaped characters are unescaped during
      splitting.

  Returns:
    A tuple of parts (strings if input was str, bytes if input was bytes).

  Raises:
    RuntimeError: If quotes are unmatched at the end of the data.

  Example:
    from py_misc_utils import split as sp

    print(sp.split('a, "b, c", d', ','))  # ('a', ' "b, c"', ' d')

    # An escaped separator is honored only with unescape=True:
    esc_sep = ',' + chr(92) + ','
    print(sp.split('x' + esc_sep + 'y', ',', unescape=True))  # ('x,y',)
    print(sp.split('x' + esc_sep + 'y', ','))  # ('x\\,y',)

    # A custom quote context, e.g. only parentheses are special:
    ctx = sp.make_context({'(': ')'})
    print(sp.split('a, (b, c)', ',', quote_ctx=ctx))  # ('a', ' (b, c)')
  """
  qctx = quote_ctx or _QUOTE_CTX

  bdata, bsplit_rx = _to_bytes(data, split_rx)
  skipper = _Skipper(qctx.quote_rx)

  sval = ord('\\')
  pos, qstack, parts, seq = 0, [], [], bytearray()
  while pos < len(bdata):
    if seq and seq[-1] == sval:
      if unescape:
        seq[-1] = bdata[pos]
      else:
        seq.append(bdata[pos])

      pos += 1
    elif qstack:
      m = re.search(qctx.quote_sprx, bdata[pos:])
      if not m:
        break

      seq.extend(bdata[pos:pos + m.start()])
      pos += m.start()
      c = bdata[pos]
      tq = qstack[-1]
      if c == tq.closec:
        qstack.pop()
      elif tq.nest_ok and (cc := qctx.map.get(c)) is not None:
        qstack.append(_Quote(cc, pos, c != cc))
      seq.append(c)
      pos += 1
    else:
      kpos, is_split = _split_forward(bdata, pos, bsplit_rx, skipper, seq)
      if is_split:
        parts.append(seq)
        seq = bytearray()
      elif kpos < len(bdata):
        c = bdata[kpos]
        if (cc := qctx.map.get(c)) is not None:
          qstack.append(_Quote(cc, kpos, c != cc))
        seq.append(c)
        kpos += 1
      pos = max(kpos, pos + 1)

  tas.check_eq(len(qstack),
               0,
               msg=f'Unmatched quotes during split: "{data}"\n  {qstack}')
  if seq or parts:
    parts.append(seq)

  return tuple(p.decode() for p in parts) if isinstance(data, str) else tuple(parts)


def unquote(data, quote_map=None):
  """Removes matching quotes from the beginning and end of a string.

  If the string starts and ends with a matching quote pair, the quotes are
  removed and any escaped occurrences of the closing quote are unescaped.

  Args:
    data: The string to unquote.
    quote_map: An optional dict mapping opening quotes to closing quotes.
      Defaults to the module-level ``_QUOTE_MAP``.

  Returns:
    The string with surrounding quotes removed, or the original string if no
    matching quote pair is found.

  Example:
    from py_misc_utils import split as sp

    print(sp.unquote('"quoted"'))  # quoted
    print(sp.unquote('[lst]'))  # lst
    print(sp.unquote('plain'))  # plain
  """
  if len(data) >= 2:
    quote_map = quote_map or _QUOTE_MAP
    cc = quote_map.get(data[0])
    if cc == data[-1]:
      udata = data[1:-1]

      return re.sub(rf'\\{cc}', rf'{cc}', udata) if cc == data[0] else udata

  return data
