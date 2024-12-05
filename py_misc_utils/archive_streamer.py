import collections
import os
import tarfile
import zipfile

from . import alog as alog
from . import assert_checks as tas
from . import gfs


ArchiveSpecs = collections.namedtuple('ArchiveSpecs', 'kind, compression, base_path')
ArchiveEntry = collections.namedtuple('ArchiveEntry', 'name, data')


def parse_specs(url):
  ubase, ext = os.path.splitext(url)
  if ext in {'.gz', '.xz', '.bz2'}:
    compression = ext[1:]
  elif ext == '.bzip2':
    compression = 'bz2'
  else:
    compression, ubase = None, url

  base_path, ext = os.path.splitext(ubase)

  tas.check(ext, msg=f'Unable to infer archive type: {url}')

  return ArchiveSpecs(kind=ext[1:], compression=compression, base_path=base_path)


class ArchiveStreamer:

  def __init__(self, url, **kwargs):
    self._url = url
    self._kwargs = kwargs

  def generate(self):
    specs = parse_specs(self._url)
    if specs.kind == 'zip':
      # The ZIP format requires random access (specifically, the file list is at EOF)
      # so it is better to cache the file locally before opening.
      with gfs.open_local(self._url, mode='rb', **self._kwargs) as stream:
        zfile = zipfile.ZipFile(stream, mode='r')
        for zinfo in zfile.infolist():
          if not zinfo.is_dir():
            data = zfile.read(zinfo)
            yield ArchiveEntry(name=zinfo.filename, data=data)
    elif specs.kind == 'tar':
      with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
        tfile = tarfile.open(mode=f'r|{specs.compression or ""}', fileobj=stream)
        for tinfo in tfile:
          data = tfile.extractfile(tinfo).read()
          yield ArchiveEntry(name=tinfo.name, data=data)
    else:
      alog.xraise(RuntimeError, f'Unknown archive type "{specs.kind}": {self._url}')

  def __iter__(self):
    return iter(self.generate())

