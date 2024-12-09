import io
import requests

import PIL.Image as Image


def from_bytes(data):
  return Image.open(io.BytesIO(data))


def from_url(url, headers=None):
  resp = requests.get(url, headers=headers)
  resp.raise_for_status()

  return from_bytes(resp.content)

