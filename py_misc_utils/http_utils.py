import re
import time


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
  return 'bytes' in headers.get(ACCEPT_RANGES, '')


def content_length(headers, defval=None):
  if (length := headers.get(XLINKED_SIZE)) is not None:
    return int(length)
  if (length := headers.get(CONTENT_LENGTH)) is not None:
    return int(length)

  return defval


def etag(headers, defval=None):
  if (etag_value := headers.get(XLINKED_ETAG)) is not None:
    return etag_value.strip('"\'')
  if (etag_value := headers.get(ETAG)) is not None:
    return etag_value.strip('"\'')

  return defval


def last_modified(headers, defval=None):
  if (mtime := headers.get(LAST_MODIFIED)) is not None:
    return date_to_epoch(mtime)

  return defval


def add_range(headers, start, stop):
  headers[RANGE] = f'bytes={start}-{stop}'

  return headers


def range(headers):
  hrange = headers.get(CONTENT_RANGE)
  if hrange is None:
    if (length := content_length(headers)) is not None:
      return 0, length - 1
  else:
    m = re.match(r'bytes\s+(\d+)\-(\d+)', hrange)
    if m:
      return int(m.group(1)), int(m.group(2))


def range_data(start, stop, headers, data):
  hrange = range(headers)
  if hrange is not None:
    rstart, rstop = hrange

    dstart = start - rstart
    size = min(stop - start, rstop - rstart) + 1
    dstop = dstart + size - 1

    if dstart != start or dstop != stop:
      return memoryview(data)[dstart: dstop + 1]

  return data


def date_to_epoch(http_date):
  htime = time.strptime(http_date, '%a, %d %b %Y %H:%M:%S %Z')

  return time.mktime(htime)

