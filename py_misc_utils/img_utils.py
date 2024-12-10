import io

import PIL.Image as Image

from . import http_utils as hu


def from_bytes(data, convert=None):
  img = Image.open(io.BytesIO(data))

  return img if convert is None else img.convert(convert)


def from_url(url, convert=None, **kwargs):
  return from_bytes(hu.get(url, **kwargs), convert=convert)

