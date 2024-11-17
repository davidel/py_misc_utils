
ACCEPT_RANGES = 'Accept-Ranges'
AUTHORIZATION = 'Authorization'
CONTENT_LENGTH = 'Content-Length'
CONTENT_TYPE = 'Content-Type'
LAST_MODIFIED = 'Last-Modified'
ETAG = 'ETag'
RANGE = 'Range'


def support_ranges(headers):
  return 'bytes' in headers.get(ACCEPT_RANGES, '')


def content_length(headers):
  if (length := headers.get(CONTENT_LENGTH)) is not None:
    return int(length)


def add_range(headers, start, stop):
  headers[RANGE] = f'bytes={start}-{stop}'

  return headers

