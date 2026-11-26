"""Tests for py_misc_utils.http_utils.

Exercises header getters, range math, HTTP-date conversion, request-arg
filtering and the info()/get() helpers against the ephemeral HTTP server.
No real network access is required: info() branch coverage uses fake
modules injected through its public mod= parameter.
"""

import time

import pytest
import requests

from py_misc_utils import http_utils as hu

# ---------------------------------------------------------------------------
# Header getters
# ---------------------------------------------------------------------------


def test_support_ranges():
  assert hu.support_ranges({'Accept-Ranges': 'bytes'})
  assert hu.support_ranges({'Accept-Ranges': 'none'}) is False
  assert hu.support_ranges({}) is False


def test_content_length_xlinked_size_precedence():
  headers = {
      'X-Linked-Size': '77',
      'Content-Length': '99',
  }
  assert hu.content_length(headers) == 77
  assert hu.content_length(headers, defval=-1) == 77


def test_content_length_fallback_and_missing():
  assert hu.content_length({'Content-Length': '42'}) == 42
  assert hu.content_length({}, defval=0) == 0
  assert hu.content_length({}) is None


def test_etag_quote_stripping():
  assert hu.etag({'ETag': '"abc"'}) == 'abc'
  assert hu.etag({'ETag': "'abc'"}) == 'abc'
  assert hu.etag({'ETag': 'abc'}) == 'abc'
  assert hu.etag({}, defval='missing') == 'missing'


def test_last_modified_parsing_and_missing():
  exp = 86400 * 365  # 1971-01-01 00:00:00 UTC
  got = hu.last_modified({'Last-Modified': 'Fri, 01 Jan 1971 00:00:00 GMT'})
  assert got == exp
  assert hu.last_modified({}, defval=-1) == -1
  assert hu.last_modified({}) is None


def test_add_range_exclusive_end():
  headers = {}
  assert hu.add_range(headers, 10, 20) is headers
  assert headers['Range'] == 'bytes=10-19'
  hu.add_range(headers, 5, 6)
  assert headers['Range'] == 'bytes=5-5'


def test_range_from_content_range():
  got = hu.range({'Content-Range': 'bytes 10-19/100'})
  assert got == hu.Range(start=10, stop=19, length=100)

  got = hu.range({'Content-Range': 'bytes 0-1023/5000'})
  assert got == hu.Range(start=0, stop=1023, length=5000)


def test_range_from_content_range_without_total():
  got = hu.range({'Content-Range': 'bytes 10-19'})
  assert got == hu.Range(start=10, stop=19, length=None)


def test_range_unsatisfiable_and_missing():
  assert hu.range({'Content-Range': 'bytes */100'}) is None
  assert hu.range({}) is None


def test_range_falls_back_to_content_length():
  got = hu.range({'Content-Length': '42'})
  assert got == hu.Range(start=0, stop=41, length=42)

  got = hu.range({'Content-Length': '0'})
  assert got == hu.Range(start=0, stop=-1, length=0)


# ---------------------------------------------------------------------------
# range_data
# ---------------------------------------------------------------------------


def _mkdata(size):
  return bytes(bytearray((idx & 255) for idx in range(size)))


def test_range_data_no_range_header_passthrough():
  data = _mkdata(64)
  assert hu.range_data(0, 63, {}, data) == data


def test_range_data_served_from_zero_sub_window():
  # A full-body response (Content-Length fallback) served from offset 0:
  # the requested sub-window must be extracted, not the whole body.
  data = _mkdata(100)
  headers = {'Content-Length': '100'}
  assert hu.range_data(10, 19, headers, data) == data[10:20]
  assert hu.range_data(0, 0, headers, data) == data[0:1]
  assert hu.range_data(95, 99, headers, data) == data[95:100]


def test_range_data_exact_match_returns_data():
  data = _mkdata(10)
  headers = {'Content-Range': 'bytes 10-19/100'}
  assert hu.range_data(10, 19, headers, data) is data


def test_range_data_partial_overlap():
  # data must be the SERVED body (the file bytes covered by the served
  # range), exactly as http_fs passes resp.content: the returned slice
  # is indexed within the served body.
  file_data = _mkdata(100)
  served = file_data[10:20]
  headers = {'Content-Range': 'bytes 10-19/100'}
  assert hu.range_data(12, 17, headers, served) == file_data[12:18]


def test_range_data_start_beyond_served_range():
  data = _mkdata(100)
  headers = {'Content-Range': 'bytes 10-19/100'}
  with pytest.raises(ValueError, match='not covered by served range'):
    hu.range_data(0, 99, headers, data)
  with pytest.raises(ValueError, match='not covered by served range'):
    hu.range_data(5, 15, headers, data)


def test_range_data_stop_beyond_served_range():
  data = _mkdata(100)
  headers = {'Content-Range': 'bytes 10-19/100'}
  with pytest.raises(ValueError, match='not covered by served range'):
    hu.range_data(10, 20, headers, data)


# ---------------------------------------------------------------------------
# HTTP dates
# ---------------------------------------------------------------------------

_EPOCH_DATE = 'Thu, 01 Jan 1970 00:00:00 GMT'


def test_epoch_to_date_epoch_zero():
  # Explicit 0 must not be mistaken for the 'no argument' default.
  assert hu.epoch_to_date(0) == _EPOCH_DATE
  assert hu.epoch_to_date(0.0) == _EPOCH_DATE


def test_epoch_to_date_default_is_now():
  got = hu.epoch_to_date(None)
  assert hu.date_to_epoch(got) == pytest.approx(time.time(), abs=5)


def test_date_to_epoch_is_timezone_independent():
  # The GMT marker must be honored: parse in UTC regardless of the
  # process timezone (the pre-fix implementation used local mktime()).
  assert hu.date_to_epoch(_EPOCH_DATE) == 0
  assert hu.date_to_epoch('Fri, 01 Jan 1971 00:00:00 GMT') == 86400 * 365


def test_date_epoch_roundtrip():
  for epoch in (0, 86400, 951782400, 1700000000):
    assert hu.date_to_epoch(hu.epoch_to_date(epoch)) == epoch


# ---------------------------------------------------------------------------
# filter_request_args
# ---------------------------------------------------------------------------


def test_filter_request_args_empty():
  assert hu.filter_request_args({}) == {}


def test_filter_request_args_collects_known_keys():
  kwargs = {
      'headers': {
          'Authorization': 'Bearer tok'
      },
      'timeout': 7,
      'auth': ('user', 'pass'),
      'cookies': {
          'sid': 'x'
      },
      'allow_redirects': False,
      'proxies': {
          'http': 'http://proxy'
      },
      'verify': False,
      'cert': '/tmp/cert.pem',
      'unknown_key': 'dropped',
  }
  got = hu.filter_request_args(kwargs)
  assert got == {key: value for key, value in kwargs.items() if key != 'unknown_key'}


def test_filter_request_args_drops_none_values():
  got = hu.filter_request_args({'headers': None, 'timeout': 5})
  assert got == {'timeout': 5}


def test_filter_request_args_prebuilt_requests_dict():
  pre = {'headers': {'X': 'y'}, 'timeout': 3}
  got = hu.filter_request_args({'requests': pre, 'timeout': 999})
  assert got is pre
  assert got['timeout'] == 3


def test_filter_request_args_custom_key():
  got = hu.filter_request_args({'fetch': {'verify': False}}, key='fetch')
  assert got == {'verify': False}


# ---------------------------------------------------------------------------
# info() / get() — fake mod injected via the public mod= parameter
# ---------------------------------------------------------------------------


class _FakeResponse:
  """Minimal duck-typed requests response for the info() code paths."""

  def __init__(self, headers, content=b''):
    self.headers = headers
    self.content = content
    self.status_code = 200

  def raise_for_status(self):
    self.status_code = 200


class _FakeMod:
  """Fake HTTP module (get/head) as accepted by info()'s mod parameter."""

  def __init__(self, get_headers=None, get_raiser=False):
    self.get_headers = get_headers
    self.get_raiser = get_raiser
    self.get_calls = 0
    self.head_calls = 0
    self.last_head_headers = None

  def get(self, url, headers=None, **kwargs):
    self.get_calls += 1
    if self.get_raiser:
      raise requests.exceptions.HTTPError('http error')
    return _FakeResponse(self.get_headers)

  def head(self, url, headers=None, **kwargs):
    self.head_calls += 1
    self.last_head_headers = headers
    return _FakeResponse({'Content-Length': '77'})


def test_info_stamps_range_support_on_range_response():
  headers = {
      'Content-Range': 'bytes 0-1023/5000',
      'Content-Length': '1024',
  }
  mod = _FakeMod(get_headers=headers)
  resp = hu.info('http://example.com/f', mod=mod)

  assert mod.get_calls == 1 and mod.head_calls == 0
  assert resp.headers[hu.CONTENT_LENGTH] == 5000
  assert resp.headers[hu.ACCEPT_RANGES] == 'bytes'
  assert hu.support_ranges(resp.headers)


def test_info_no_false_range_support_on_plain_response():
  # A server that ignores Range and answers a plain 200 with the full
  # body must NOT be reported as range-capable (the pre-fix bug that
  # misled http_fs into block reads against such servers).
  mod = _FakeMod(get_headers={'Content-Length': '100'})
  resp = hu.info('http://example.com/f', mod=mod)

  assert mod.get_calls == 1 and mod.head_calls == 1
  assert hu.support_ranges(resp.headers) is False
  assert resp.headers[hu.CONTENT_LENGTH] == '77'


def test_info_falls_back_to_head_on_http_error():
  mod = _FakeMod(get_raiser=True)
  caller_headers = {'Authorization': 'Bearer tok'}
  hu.info('http://example.com/f', headers=caller_headers, mod=mod)

  assert mod.get_calls == 1 and mod.head_calls == 1
  assert mod.last_head_headers is caller_headers


# ---------------------------------------------------------------------------
# get() — against the ephemeral HTTP server
# ---------------------------------------------------------------------------


def test_get_returns_content(http_server, http_routes):
  http_routes.add_route('/data.bin', body=b'payload-bytes')
  got = hu.get(http_server.url + '/data.bin')
  assert got == b'payload-bytes'


def test_get_raises_on_http_error(http_server, http_routes):
  with pytest.raises(requests.exceptions.HTTPError):
    hu.get(http_server.url + '/no-such-route')
