"""Tests for py_misc_utils.img_utils.

from_bytes is exercised with locally generated PIL images; from_url is
served by the ephemeral conftest HTTP server, so no network access is
required.
"""

import io

import PIL.Image
import pytest

from py_misc_utils import img_utils as iu


def _png(mode):
  """Return the PNG-encoded bytes of a 2x3 image in the given mode."""
  buf = io.BytesIO()
  color = (10, 20, 30) if mode == 'RGB' else 99
  PIL.Image.new(mode, (2, 3), color).save(buf, format='PNG')

  return buf.getvalue()


def test_from_bytes_png():
  img = iu.from_bytes(_png('RGB'))
  assert isinstance(img, PIL.Image.Image)
  assert img.size == (2, 3)
  assert img.mode == 'RGB'


def test_from_bytes_grayscale():
  img = iu.from_bytes(_png('L'))
  assert img.size == (2, 3)
  assert img.mode == 'L'


def test_from_bytes_convert():
  img = iu.from_bytes(_png('L'), convert='RGB')
  assert img.mode == 'RGB'
  assert img.size == (2, 3)


def test_from_bytes_convert_same_mode():
  img = iu.from_bytes(_png('RGB'), convert='RGB')
  assert img.mode == 'RGB'
  assert img.getpixel((0, 0)) == (10, 20, 30)


def test_from_bytes_invalid():
  with pytest.raises(Exception) as excinfo:
    iu.from_bytes(b'not an image at all')

  # The original loader error type is preserved and a note about the data
  # prefix is attached.
  assert not isinstance(excinfo.value, AssertionError)
  notes = getattr(excinfo.value, '__notes__', [])
  assert any('Unable to load image' in n for n in notes)


def test_from_bytes_invalid_prefix_shown():
  data = b'PREFIX-data-not-image'
  with pytest.raises(Exception) as excinfo:
    iu.from_bytes(data)

  notes = getattr(excinfo.value, '__notes__', [])
  assert any(repr(data[:16]) in n or str(data[:16]) in n for n in notes)


def test_from_url(http_routes):
  http_routes.add_route('/img.png', _png('RGB'))
  img = iu.from_url(http_routes.url + '/img.png')
  assert isinstance(img, PIL.Image.Image)
  assert img.size == (2, 3)
  assert img.mode == 'RGB'


def test_from_url_convert(http_routes):
  http_routes.add_route('/img.png', _png('L'))
  img = iu.from_url(http_routes.url + '/img.png', convert='RGB')
  assert img.mode == 'RGB'
