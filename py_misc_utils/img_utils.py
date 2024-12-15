import io

import PIL.Image as Image

from . import http_utils as hu


def from_bytes(data, convert=None):
  try:
    img = Image.open(io.BytesIO(data))
  except Exception as ex:
    ex.add_note(f'Unable to load image: data={data[: 16]}...')
    raise

  return img if convert is None else img.convert(convert)


def from_url(url, convert=None, **kwargs):
  return from_bytes(hu.get(url, **kwargs), convert=convert)

