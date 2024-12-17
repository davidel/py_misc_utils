import msgpack

from . import alog as alog
from . import gfs
from . import utils as ut


class MsgPackStreamer:

  def __init__(self, url, rename_columns=None, **kwargs):
    self._url = url
    self._rename_columns = ut.value_or(rename_columns, dict())
    self._kwargs = kwargs

  def generate(self):
    with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
      unpacker = msgpack.Unpacker(stream)
      for recd in unpacker:
        for key, name in self._rename_columns.items():
          recd[name] = recd.pop(key)

        yield recd

  def __iter__(self):
    return iter(self.generate())

