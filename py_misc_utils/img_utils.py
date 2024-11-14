import io

import PIL.Image as Image


def from_bytes(data):
  return Image.open(io.BytesIO(data))

