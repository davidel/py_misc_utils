import io

import PIL.Image as Image

from . import http_utils as hu


def from_bytes(data):
  return Image.open(io.BytesIO(data))


def from_url(url, **kwargs):
  return from_bytes(hu.get(url, **kwargs))

