import collections
import hashlib
import os
import requests
import tarfile
import zipfile

from . import alog as alog
from . import assert_checks as tas
from . import gfs
from . import http_utils as hu


ArchiveSpecs = collections.namedtuple('ArchiveSpecs', 'kind, compression, base_path')
ArchiveEntry = collections.namedtuple('ArchiveEntry', 'name, data')


def parse_specs(url):
  ubase, ext, purl = hu.url_splitext(url)

  if ext in {'.gz', '.xz', '.bz2'}:
    compression = ext[1:]
  elif ext == '.bzip2':
    compression = 'bz2'
  else:
    compression, ubase = None, purl.path

  base_path, ext = os.path.splitext(ubase)

  tas.check(ext, msg=f'Unable to infer archive type: {url}')

  return ArchiveSpecs(kind=ext[1:], compression=compression, base_path=base_path)


class ArchiveStreamer:

  def __init__(self, url, **kwargs):
    self._url = url
    self._kwargs = kwargs

  def _generate_zip(self, specs):
    # The ZIP format requires random access (specifically, the file list is at EOF)
    # so it is better to cache the file locally before opening.
    with gfs.open_local(self._url, mode='rb', **self._kwargs) as stream:
      zfile = zipfile.ZipFile(stream, mode='r')
      for zinfo in zfile.infolist():
        if not zinfo.is_dir():
          data = zfile.read(zinfo)
          yield ArchiveEntry(name=zinfo.filename, data=data)

  def _generate_tar(self, specs):
    with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
      tfile = tarfile.open(mode=f'r|{specs.compression or ""}', fileobj=stream)
      for tinfo in tfile:
        data = tfile.extractfile(tinfo).read()
        yield ArchiveEntry(name=tinfo.name, data=data)

  def _create_parquet_entries(self, ddf, ruid, n, session):
    entries = []
    for col, values in ddf.items():
      data = values[n]

      entries.append(ArchiveEntry(name=f'{ruid}.{col}', data=data))
      if col == 'url':
        url_data = hu.get(data, headers=self._kwargs.get('headers'), mod=session)
        ext = hu.url_splitext(data)[1]
        entries.append(ArchiveEntry(name=f'{ruid}.{ext[1:].lower()}', data=url_data))

    return entries

  def _generate_parquet(self, specs, batch_size=16):
    # Keep the import dependency local, to make it required only if parquet is used.
    import pyarrow.parquet as pq

    session = requests.Session()
    uid = hashlib.sha1(self._url.encode()).hexdigest()[: 16]
    with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
      nrecs = 0
      pqfd = pq.ParquetFile(stream)
      for rec in pqfd.iter_batches(batch_size=batch_size):
        ddf = rec.to_pydict()
        for n in range(len(rec)):
          ruid = f'{uid}_{nrecs}'

          try:
            entries = self._create_parquet_entries(ddf, ruid, n, session)
            for aentry in entries:
              yield aentry

            nrecs += 1
          except requests.exceptions.RequestException:
            pass

  def generate(self):
    specs = parse_specs(self._url)
    if specs.kind == 'zip':
      yield from self._generate_zip(specs)
    elif specs.kind == 'tar':
      yield from self._generate_tar(specs)
    elif specs.kind == 'parquet':
      yield from self._generate_parquet(specs)
    else:
      alog.xraise(RuntimeError, f'Unknown archive type "{specs.kind}": {self._url}')

  def __iter__(self):
    return iter(self.generate())

