"""HTTP utility functions for handling range requests, headers, and responses.

This module provides utilities for:
- Range request/response handling (partial content)
- Header parsing (Content-Length, ETag, Last-Modified, etc.)
- HTTP date conversion
- Request argument filtering
- Convenience functions for GET/HEAD requests with range support
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
  """
  return 'bytes' in headers.get(ACCEPT_RANGES, '')


def content_length(headers, defval=None):
  """Get content length from headers, checking X-Linked-Size first.

  Args:
    headers: HTTP response headers dictionary.
    defval: Default value if not found (default: None).

  Returns:
    Content length as int, or defval.
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
  """
  headers[RANGE] = f'bytes={start}-{end - 1}'

  return headers


Range = collections.namedtuple('Range', 'start, stop, length')


def range(headers):
  """Parse Content-Range header or infer range from Content-Length.

  Args:
    headers: HTTP response headers dictionary.

  Returns:
    Range namedtuple (start, stop, length) or None if not available.
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
    stop: The requested stop offset.
    headers: The range resolved from the request headers, as per the
      add_range()/range() helpers.
    data: The full contents served by the request.

  Returns:
    The data slice matching the range, or the data itself when no
    clipping is needed.
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
    Epoch timestamp as float.
  """
  htime = time.strptime(http_date, _HTTP_DATE_FMT)

  return calendar.timegm(htime)


def epoch_to_date(epoch_time=None):
  """Convert epoch timestamp to HTTP date string.

  Args:
    epoch_time: Epoch timestamp (default: current time).

  Returns:
    Date string in HTTP format (e.g., 'Mon, 01 Jan 2024 00:00:00 GMT').
  """
  return time.strftime(_HTTP_DATE_FMT,
                       time.gmtime(time.time() if epoch_time is None else epoch_time))


def filter_request_args(kwargs, key=None):
  """Extract requests-compatible arguments from a keyword dictionary.

  Args:
    kwargs: Dictionary containing request parameters.
    key: Key in kwargs that contains a pre-built requests kwargs dict
      (default: 'requests').

  Returns:
    Dictionary with keys: headers, timeout, auth, cookies, allow_redirects,
    proxies, verify, cert.
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
    mod: HTTP module to use (default: requests).
    headers: Optional request headers (default: empty dict).
    **kwargs: Additional arguments passed to mod.get()/mod.head().

  Returns:
    Response object with Content-Length, ETag, Last-Modified headers populated.
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
  """
  mod = mod or requests

  resp = mod.get(url, **kwargs)
  resp.raise_for_status()

  return resp.content
