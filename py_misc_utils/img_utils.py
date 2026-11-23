"""Image loading utilities using PIL/Pillow.

This module provides functions to load images from bytes or URLs using PIL,
with optional format conversion.
"""

import io

import PIL.Image as Image

from . import http_utils as hu


def from_bytes(data, convert=None):
  """Load an image from bytes using PIL.

  Args:
    data: Raw image bytes.
    convert: Optional PIL conversion mode string (e.g. 'RGB', 'L').
      Defaults to None (no conversion).

  Returns:
    A PIL Image object.

  Raises:
    Exception: If the image cannot be loaded, with an added note about
      the data prefix.
  """
  try:
    img = Image.open(io.BytesIO(data))
  except Exception as ex:
    ex.add_note(f'Unable to load image: data={data[: 16]}...')
    raise

  return img if convert is None else img.convert(convert)


def from_url(url, convert=None, **kwargs):
  """Load an image from a URL using PIL.

  Fetches the URL content via http_utils.get() and loads it as an image.

  Args:
    url: The URL string to fetch the image from.
    convert: Optional PIL conversion mode string (e.g. 'RGB', 'L').
      Defaults to None (no conversion).
    **kwargs: Additional keyword arguments passed to hu.get().

  Returns:
    A PIL Image object.
  """
  return from_bytes(hu.get(url, **kwargs), convert=convert)
