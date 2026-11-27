"""HTTP utility functions for handling range requests, headers, and responses.

This module provides utilities for:
- Range request/response handling (partial content)
- Header parsing (Content-Length, ETag, Last-Modified, etc.)
- HTTP date conversion
- Request argument filtering
- Convenience functions for GET/HEAD requests with range support

Example:
  import py_misc_utils.http_utils as hu

  hdrs = {'Content-Range': 'bytes 512-1535/4096'}
  r = hu.range(hdrs)
  assert r == (512, 1535, 4096)
"""

import calendar
import collections
import re
import time

import requests

ACCEPT_RANGES = 'Accept-Ranges'
AUTHORIZATION = 'Authorization'
CONTENT_LENGTH = 'Content-Length'
CONTENT_TYPE = 'Content-Type'
CONTENT_ENCODING = 'Content-Encoding'
LAST_MODIFIED = 'Last-Modified'
XLINKED_SIZE = 'X-Linked-Size'
XLINKED_ETAG = 'X-Linked-ETag'
ETAG = 'ETag'
RANGE = 'Range'
CONTENT_RANGE = 'Content-Range'


def support_ranges(headers):
  """Check if the server supports range requests.

  Args:
    headers: HTTP response headers dictionary.

  Returns:
    bool: True if 'bytes' is in Accept-Ranges header.

  Example:
    import py_misc_utils.http_utils as hu

    assert hu.support_ranges({'Accept-Ranges': 'bytes'})
    assert not hu.support_ranges({'Accept-Ranges': 'none'})
    assert not hu.support_ranges({})
  """
  return 'bytes' in headers.get(ACCEPT_RANGES, '')


def content_length(headers, defval=None):
  """Get content length from headers, checking X-Linked-Size first.

  The X-Linked-Size header - as served by some cloud providers to expose
  the size of compressed content - takes precedence over Content-Length.

  Args:
    headers: HTTP response headers dictionary.
    defval: Default value if not found (default: None).

  Returns:
    Content length as int, or defval.

  Example:
    import py_misc_utils.http_utils as hu

    assert hu.content_length({'Content-Length': '128'}) == 128
    assert hu.content_length(
        {'X-Linked-Size': '4096', 'Content-Length': '128'}) == 4096
    assert hu.content_length({}) is None
    assert hu.content_length({}, defval=0) == 0
  """
  if (length := headers.get(XLINKED_SIZE)) is not None:
    return int(length)
  if (length := headers.get(CONTENT_LENGTH)) is not None:
    return int(length)

  return defval


def etag(headers, defval=None):
  """Get ETag from headers, checking X-Linked-ETag first.

  Args:
    headers: HTTP response headers dictionary.
    defval: Default value if not found (default: None).

  Returns:
    ETag string (stripped of quotes), or defval.

  Example:
    import py_misc_utils.http_utils as hu

    assert hu.etag({'ETag': '"abc123"'}) == 'abc123'
    # Weak validators keep their W/ prefix - only the quotes are stripped.
    assert hu.etag({'ETag': 'W/"abc123"'}) == 'W/"abc123'
    assert hu.etag({}) is None
    assert hu.etag({}, defval='') == ''
  """
  if (etag_value := headers.get(XLINKED_ETAG)) is not None:
    return etag_value.strip('"\'')
  if (etag_value := headers.get(ETAG)) is not None:
    return etag_value.strip('"\'')

  return defval


def last_modified(headers, defval=None):
  """Get last modified time from headers as epoch timestamp.

  Args:
    headers: HTTP response headers dictionary.
    defval: Default value if not found (default: None).

  Returns:
    Last-Modified time as epoch timestamp (float), or defval.

  Example:
    import py_misc_utils.http_utils as hu

    mt = hu.last_modified({'Last-Modified': 'Wed, 21 Oct 2015 07:28:00 GMT'})
    assert mt == 1445412480.0
    assert hu.last_modified({}) is None
  """
  if (mtime := headers.get(LAST_MODIFIED)) is not None:
    return date_to_epoch(mtime)

  return defval


def add_range(headers, start, end):
  """Add a Range header to request headers.

  Args:
    headers: Request headers dictionary (modified in place).
    start: Start byte (inclusive).
    end: End byte (exclusive).

  Returns:
    The modified headers dictionary.

  Example:
    import py_misc_utils.http_utils as hu

    headers = hu.add_range({}, 1024, 2048)
    assert headers['Range'] == 'bytes=1024-2047'
  """
  headers[RANGE] = f'bytes={start}-{end - 1}'

  return headers


Range = collections.namedtuple('Range', 'start, stop, length')


def range(headers):
  """Parse Content-Range header or infer range from Content-Length.

  Args:
    headers: HTTP response headers dictionary.

  Returns:
    Range namedtuple (start, stop, length) - where stop is the last byte
    offset (inclusive) - or None if not available.

  Example:
    import py_misc_utils.http_utils as hu

    r = hu.range({'Content-Range': 'bytes 512-1535/4096'})
    assert r == (512, 1535, 4096)
    assert r.start == 512 and r.stop == 1535 and r.length == 4096

    # Without Content-Range, the range is inferred from Content-Length.
    assert hu.range({'Content-Length': '128'}) == (0, 127, 128)
    assert hu.range({}) is None
  """
  hrange = headers.get(CONTENT_RANGE)
  if hrange is None:
    if (length := content_length(headers)) is not None:
      return Range(start=0, stop=length - 1, length=length)
  else:
    m = re.match(r'bytes\s+(\d+)\-(\d+)(/(\d+))?', hrange)
    if m:
      hlength = m.group(4)
      length = int(hlength) if hlength else None

      return Range(start=int(m.group(1)), stop=int(m.group(2)), length=length)


def range_data(start, stop, headers, data):
  """Return the slice of data matching a possibly clipped range.

  Args:
    start: The requested start offset.
    stop: The requested stop offset (inclusive).
    headers: The response headers holding the range actually served - as
      parsed by the range() helper.
    data: The contents served by the response, covering the served range.

  Returns:
    The data slice matching the requested range, or the data itself when
    no clipping is needed.

  Raises:
    ValueError: If the requested range is not covered by the served range.

  Example:
    import py_misc_utils.http_utils as hu

    data = b'0123456789'
    hdrs = {'Content-Range': 'bytes 2-5/10'}

    assert bytes(hu.range_data(2, 5, hdrs, b'2345')) == b'2345'
    # A sub-range of the served range is clipped out of it.
    assert bytes(hu.range_data(2, 3, hdrs, b'2345')) == b'23'
    # Without Content-Range the whole data is served - and returned.
    assert bytes(hu.range_data(0, 9, {}, data)) == data
  """
  hrange = range(headers)
  if hrange is not None:
    if start < hrange.start or stop > hrange.stop:
      from . import alog

      alog.xraise(ValueError,
                  f'Requested range [{start}, {stop}] not covered by served '
                  f'range [{hrange.start}, {hrange.stop}]')

    dstart = start - hrange.start
    size = min(stop - start, hrange.stop - hrange.start) + 1
    dstop = dstart + size - 1

    if dstart != 0 or dstop != len(data) - 1:
      return memoryview(data)[dstart:dstop + 1]

  return data


_HTTP_DATE_FMT = '%a, %d %b %Y %H:%M:%S %Z'


def date_to_epoch(http_date):
  """Convert HTTP date string to epoch timestamp.

  Args:
    http_date: Date string in HTTP format (e.g., 'Mon, 01 Jan 2024 00:00:00 GMT').

  Returns:
    Epoch timestamp as int.

  Example:
    import py_misc_utils.http_utils as hu

    assert hu.date_to_epoch('Wed, 21 Oct 2015 07:28:00 GMT') == 1445412480
  """
  htime = time.strptime(http_date, _HTTP_DATE_FMT)

  return calendar.timegm(htime)


def epoch_to_date(epoch_time=None):
  """Convert epoch timestamp to HTTP date string.

  Args:
    epoch_time: Epoch timestamp (default: current time).

  Returns:
    Date string in HTTP format (e.g., 'Mon, 01 Jan 2024 00:00:00 GMT').

  Example:
    import py_misc_utils.http_utils as hu

    assert hu.epoch_to_date(1445412480) == 'Wed, 21 Oct 2015 07:28:00 GMT'
    # Round-trips with date_to_epoch().
    assert hu.date_to_epoch(hu.epoch_to_date(1445412480)) == 1445412480
  """
  return time.strftime(_HTTP_DATE_FMT,
                       time.gmtime(time.time() if epoch_time is None else epoch_time))


def filter_request_args(kwargs, key=None):
  """Extract requests-compatible arguments from a keyword dictionary.

  Args:
    kwargs: Dictionary containing request parameters - only the known
      requests arguments (headers, timeout, auth, cookies, allow_redirects,
      proxies, verify, cert) are picked up.
    key: Key in kwargs holding a pre-built requests kwargs dict, which is
      returned as-is (default: 'requests').

  Returns:
    The requests-compatible arguments dictionary.

  Example:
    import py_misc_utils.http_utils as hu

    args = hu.filter_request_args(
        {'headers': {'X-Token': 'tok'}, 'timeout': 4.0, 'bogus': 'x'})
    assert args == {'headers': {'X-Token': 'tok'}, 'timeout': 4.0}

    # A pre-built requests dict, under key, is returned as-is.
    args2 = hu.filter_request_args({'client': {'headers': {'a': 'b'}}},
                                    key='client')
    assert args2 == {'headers': {'a': 'b'}}
  """
  req_kwargs = kwargs.get(key or 'requests')
  if req_kwargs is None:
    req_kwargs = dict()
    for arg in ('headers', 'timeout', 'auth', 'cookies', 'allow_redirects', 'proxies',
                'verify', 'cert'):
      argv = kwargs.get(arg)
      if argv is not None:
        req_kwargs[arg] = argv

  return req_kwargs


def info(url, mod=None, headers=None, **kwargs):
  """Get file metadata (size, ETag, Last-Modified) using HEAD or range GET.

  Attempts a GET request with a small range (0-1023) first to detect range
  support and get metadata. Falls back to HEAD if range request fails.

  Args:
    url: URL to request.
    mod: The requests-like module used to perform the requests (default:
      requests). For testing, any object with get()/head() methods
      returning a requests-like response can be passed.
    headers: Optional request headers (default: empty dict).
    **kwargs: Additional arguments passed to mod.get()/mod.head().

  Returns:
    The response of the metadata request - with Content-Length holding the
    file size, and possibly ETag and Last-Modified headers populated.

  Example:
    import py_misc_utils.http_utils as hu


    class FakeResp:
      def __init__(self, headers, content):
        self.headers = headers
        self.content = content

      def raise_for_status(self):
        pass


    class FakeMod:
      def __init__(self, headers, content):
        self.headers = headers
        self.content = content

      def get(self, url, **kwargs):
        return FakeResp(self.headers, self.content)

      def head(self, url, **kwargs):
        return FakeResp(self.headers, b'')


    mod = FakeMod({'Content-Range': 'bytes 0-1023/8192',
                   'ETag': '"v1"',
                   'Last-Modified': 'Wed, 21 Oct 2015 07:28:00 GMT'},
                  b'x' * 1024)
    info = hu.info('http://host/data.bin', mod=mod)
    assert info.headers['Content-Length'] == 8192
    assert hu.etag(info.headers) == 'v1'
    assert hu.last_modified(info.headers) == 1445412480
  """
  mod = mod or requests
  req_headers = headers.copy() if headers else dict()

  add_range(req_headers, 0, 1024)

  try:
    resp = mod.get(url, headers=req_headers, **kwargs)
    resp.raise_for_status()

    hrange = range(resp.headers)
    if hrange is not None and hrange.length is not None and CONTENT_RANGE in resp.headers:
      resp.headers[CONTENT_LENGTH] = hrange.length
      resp.headers[ACCEPT_RANGES] = 'bytes'
    else:
      resp = None
  except requests.exceptions.HTTPError:
    resp = None

  if resp is None:
    resp = mod.head(url, headers=headers, **kwargs)
    resp.raise_for_status()

  return resp


def get(url, mod=None, **kwargs):
  """Send a GET request and return the response content.

  Args:
    url: URL to request.
    mod: HTTP module to use (default: requests).
    **kwargs: Additional arguments passed to mod.get().

  Returns:
    Response content as bytes.

  Raises:
    requests.exceptions.HTTPError: If the server returns an error status.

  Example:
    import py_misc_utils.http_utils as hu


    class FakeResp:
      def __init__(self, content):
        self.content = content

      def raise_for_status(self):
        pass


    class FakeMod:
      def get(self, url, **kwargs):
        return FakeResp(b'fake-payload')

    assert hu.get('http://host/data.bin', mod=FakeMod()) == b'fake-payload'
  """
  mod = mod or requests

  resp = mod.get(url, **kwargs)
  resp.raise_for_status()

  return resp.content
