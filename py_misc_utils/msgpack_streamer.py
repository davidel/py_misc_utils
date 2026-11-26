"""MessagePack streaming reader for line-delimited records.

This module provides :class:`MsgPackStreamer` which reads MessagePack-encoded
records from a file or stream, with optional column renaming. It yields each
decoded record as a dictionary, supporting iteration over large files without
loading everything into memory.
"""

import msgpack

from . import gfs
from . import utils as ut


class MsgPackStreamer:
  """Stream MessagePack-encoded records from a file or URL.
  """

  def __init__(self, url, rename_columns=None, **kwargs):
    """Initialize the streamer with a source URL and optional column renaming.

    Args:
      url: The file path or URL to read MessagePack records from.
      rename_columns: Optional mapping of old column names to new column names.
      **kwargs: Additional keyword arguments passed to gfs.open.
    """
    self._url = url
    self._rename_columns = ut.value_or(rename_columns, dict())
    self._kwargs = kwargs

  def generate(self):
    """Yield decoded MessagePack records from the source stream.

    Each record is yielded as a dictionary with renamed columns applied
    according to the _rename_columns mapping.

    Yields:
      Decoded record dictionaries from the MessagePack stream.
    """
    with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
      unpacker = msgpack.Unpacker(stream)
      for recd in unpacker:
        for key, name in self._rename_columns.items():
          recd[name] = recd.pop(key)

        yield recd

  def __iter__(self):
    """Return an iterator over the unpacked records of the stream."""
    return self.generate()
