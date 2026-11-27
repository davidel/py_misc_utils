"""Image loading utilities using PIL/Pillow.

This module provides functions to load images from bytes or URLs using PIL,
with optional format conversion.

Example:
  import io

  from PIL import Image

  from py_misc_utils import img_utils as iu

  buf = io.BytesIO()
  Image.new('RGB', (16, 8), color=(200, 10, 10)).save(buf, format='PNG')

  img = iu.from_bytes(buf.getvalue())
  assert img.size == (16, 8) and img.mode == 'RGB'
"""

import io

from PIL import Image

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

  Example:
    import io

    from PIL import Image

    from py_misc_utils import img_utils as iu

    buf = io.BytesIO()
    Image.new('RGB', (16, 8), color=(200, 10, 10)).save(buf, format='PNG')

    img = iu.from_bytes(buf.getvalue())
    assert img.size == (16, 8) and img.mode == 'RGB'

    gray = iu.from_bytes(buf.getvalue(), convert='L')
    assert gray.mode == 'L'
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
    **kwargs: Additional keyword arguments passed to hu.get() - notably
      the mod= parameter, to use a different requests-like module.

  Returns:
    A PIL Image object.

  Example:
    import io

    from PIL import Image

    from py_misc_utils import img_utils as iu


    class FakeResp:
      def __init__(self, content):
        self.content = content

      def raise_for_status(self):
        pass


    buf = io.BytesIO()
    Image.new('RGB', (4, 2), color=(1, 2, 3)).save(buf, format='PNG')
    data = buf.getvalue()


    class FakeMod:
      def get(self, url, **kwargs):
        return FakeResp(data)

    img = iu.from_url('http://host/pic.png', mod=FakeMod())
    assert img.size == (4, 2) and img.mode == 'RGB'
  """
  return from_bytes(hu.get(url, **kwargs), convert=convert)
