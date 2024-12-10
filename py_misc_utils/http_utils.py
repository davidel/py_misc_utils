import collections
import os
import re
import requests
import time
import urllib.parse as uparse


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


def add_range(headers, start, end):
  headers[RANGE] = f'bytes={start}-{end - 1}'

  return headers


Range = collections.namedtuple('Range', 'start, stop, length')

def range(headers):
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
  hrange = range(headers)
  if hrange is not None:
    dstart = start - hrange.start
    size = min(stop - start, hrange.stop - hrange.start) + 1
    dstop = dstart + size - 1

    if dstart != start or dstop != stop:
      return memoryview(data)[dstart: dstop + 1]

  return data


_HTTP_DATE_FMT ='%a, %d %b %Y %H:%M:%S %Z'

def date_to_epoch(http_date):
  htime = time.strptime(http_date, _HTTP_DATE_FMT)

  return time.mktime(htime)


def epoch_to_date(epoch_time=None):
  return time.strftime(_HTTP_DATE_FMT, time.gmtime(epoch_time or time.time()))


def info(url, headers=None, mod=None):
  mod = mod or requests
  req_headers = headers.copy() if headers else dict()

  add_range(req_headers, 0, 1024)

  try:
    resp = mod.get(url, headers=req_headers)
    resp.raise_for_status()

    hrange = range(resp.headers)
    if hrange is not None and hrange.length is not None:
      resp.headers[CONTENT_LENGTH] = hrange.length
      resp.headers[ACCEPT_RANGES] = 'bytes'
    else:
      resp = None
  except requests.exceptions.HTTPError:
    resp = None

  if resp is None:
    resp = mod.head(url, headers=headers)
    resp.raise_for_status()

  return resp


def get(url, headers=None, mod=None):
  mod = mod or requests

  resp = mod.get(url, headers=headers)
  resp.raise_for_status()

  return resp.content


def url_splitext(url):
  purl = uparse.urlparse(url)

  return *os.path.splitext(purl.path), purl

