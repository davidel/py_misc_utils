import time


ACCEPT_RANGES = 'Accept-Ranges'
AUTHORIZATION = 'Authorization'
CONTENT_LENGTH = 'Content-Length'
CONTENT_TYPE = 'Content-Type'
LAST_MODIFIED = 'Last-Modified'
XLINKED_SIZE = 'X-Linked-Size'
XLINKED_ETAG = 'X-Linked-ETag'
ETAG = 'ETag'
RANGE = 'Range'


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
    return etag_value
  if (etag_value := headers.get(ETAG)) is not None:
    return etag_value

  return defval


def add_range(headers, start, stop):
  headers[RANGE] = f'bytes={start}-{stop}'

  return headers


def date_to_epoch(http_date):
  htime = time.strptime(tt, '%a, %d %b %Y %I:%M:%S %Z')

  return time.mktime(htime)

