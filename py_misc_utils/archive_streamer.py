"""Archive-like streaming of data files.

This module provides ArchiveStreamer, an iterator over the data files of
an archive: besides the classic zip and tar (with optional compression)
formats, parquet and msgpack files are streamed as well - with each
record generating a set of UID.ENTITY-named entries, similar to what a
Web Dataset would expect.
"""
import collections
import hashlib
import os
import tarfile
import zipfile

from . import alog
from . import assert_checks as tas
from . import gfs

ArchiveSpecs = collections.namedtuple('ArchiveSpecs',
                                      'kind, compression, base_path, purl')
# Archive type specs: kind (zip/tar/parquet/msgpack), compression
# (zero or one of gz/xz/bz2), base_path (the URL without archive and
# compression extensions) and purl (the parsed URL).

ArchiveEntry = collections.namedtuple('ArchiveEntry', 'name, data')
# An archive data file: its name and content (as bytes).

_EXT_COMPRESSION = {
    'gz': 'gz',
    'xz': 'xz',
    'bz2': 'bz2',
    'bzip2': 'bz2',
}


def parse_specs(url):
  """Parses an archive URL into its archive specs.

  Args:
    url: The archive URL - with its archive-type extension, optionally
      followed by a compression extension.

  Returns:
    The ArchiveSpecs of the archive.

  Raises:
    AssertionError: If the archive type extension cannot be inferred.
  """
  usplit = gfs.splitext(url)

  compression = _EXT_COMPRESSION.get(usplit.ext)
  ubase = usplit.base if compression else usplit.purl.path

  base_path, ext = os.path.splitext(ubase)

  tas.check(ext, msg=f'Unable to infer archive type: {url}')

  return ArchiveSpecs(kind=usplit.ext.lower(),
                      compression=compression,
                      base_path=base_path,
                      purl=usplit.purl)


class ArchiveStreamer:
  """An iterator over the data files of an archive.

  Supported archive types: zip, tar (optionally compressed with
  gz/xz/bz2), parquet and msgpack files.
  """

  def __init__(self, url, **kwargs):
    """Creates an archive streamer.

    Args:
      url: The archive URL.
      **kwargs: Keyword arguments passed to the archive accessors - like
        the fetch options of the remote filesystems.
    """
    self._url = url
    self._kwargs = kwargs

  def _url_uid(self, url):
    """Returns the unique id - a hash - of an URL."""
    return hashlib.sha1(url.encode()).hexdigest()[:8]

  def _generate_zip(self, specs):
    """Generates the entries of a zip archive."""
    # The ZIP format requires random access (specifically, the file list is at EOF)
    # so it is better to cache the file locally before opening.
    with gfs.open_local(self._url, mode='rb', **self._kwargs) as stream:
      zfile = zipfile.ZipFile(stream, mode='r')
      for zinfo in zfile.infolist():
        if not zinfo.is_dir():
          data = zfile.read(zinfo)
          yield ArchiveEntry(name=zinfo.filename, data=data)

  def _generate_tar(self, specs):
    """Generates the entries of a - possibly compressed - tar archive."""
    with (gfs.open(self._url, mode='rb', **self._kwargs) as
          stream, tarfile.open(mode=f'r|{specs.compression or ""}',
                               fileobj=stream) as tfile):
      for tinfo in tfile:
        data = tfile.extractfile(tinfo).read()
        yield ArchiveEntry(name=tinfo.name, data=data)

  def _generate_parquet(self, specs):
    """Generates the entries of a parquet file."""
    # Keep the import dependency local, to make it required only if parquet is used.
    from . import parquet_streamer as pqs

    uid = self._url_uid(self._url)

    pq_streamer = pqs.ParquetStreamer(self._url, **self._kwargs)
    for i, recd in enumerate(pq_streamer):
      # Simulate a streaming similar to what a Web Dataset would expect, with a
      # UID.ENTITY naming, where the UID is constant for all the entities of a record
      # (which are streamed sequentially).
      ruid = f'{uid}_{i}'
      for name, data in recd.items():
        yield ArchiveEntry(name=f'{ruid}.{name}', data=data)

  def _generate_msgpack(self, specs):
    """Generates the entries of a msgpack file."""
    # Keep the import dependency local, to make it required only if parquet is used.
    from . import msgpack_streamer as mps

    uid = self._url_uid(self._url)

    mps_streamer = mps.MsgPackStreamer(self._url, **self._kwargs)
    for i, recd in enumerate(mps_streamer):
      # Simulate a streaming similar to what a Web Dataset would expect, with a
      # UID.ENTITY naming, where the UID is constant for all the entities of a record
      # (which are streamed sequentially).
      ruid = f'{uid}_{i}'
      for name, data in recd.items():
        yield ArchiveEntry(name=f'{ruid}.{name}', data=data)

  def generate(self):
    """Generates the data files of the archive.

    Returns:
      A generator yielding ArchiveEntry objects - one per data file -
      whose kind is selected by the archive URL extension.
    """
    specs = parse_specs(self._url)
    if specs.kind == 'zip':
      yield from self._generate_zip(specs)
    elif specs.kind == 'tar':
      yield from self._generate_tar(specs)
    elif specs.kind == 'parquet':
      yield from self._generate_parquet(specs)
    elif specs.kind == 'msgpack':
      yield from self._generate_msgpack(specs)
    else:
      alog.xraise(RuntimeError, f'Unknown archive type "{specs.kind}": {self._url}')

  def __iter__(self):
    """Returns the archive data files generator."""

    return self.generate()
