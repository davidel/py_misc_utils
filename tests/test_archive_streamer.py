"""Tests for py_misc_utils.archive_streamer.

Expectations pinned to the observed behavior: ArchiveStreamer iterates
over archive entries yielding ArchiveEntry(name, data) namedtuples;
supports .zip, .tar(.gz/.bz2/.xz), .msgpack formats.
"""

import io
import os
import tarfile
import zipfile

import msgpack

from py_misc_utils import archive_streamer as ars


def _write_zip(path, files):
  with zipfile.ZipFile(path, 'w') as zf:
    for name, data in files.items():
      zf.writestr(name, data)


def _write_tar(path, files, mode='w'):
  with tarfile.open(path, mode) as tf:
    for name, data in files.items():
      info = tarfile.TarInfo(name=name)
      info.size = len(data)
      tf.addfile(info, io.BytesIO(data))


def _write_msgpack(path, records):
  with open(path, 'wb') as fd:
    fd.writelines(msgpack.packb(rec) for rec in records)


def test_archive_streamer_zip(tmp_path):
  """ArchiveStreamer yields entries from a zip archive."""
  path = os.path.join(tmp_path, 'data.zip')
  files = {'a.txt': b'hello a', 'b.txt': b'hello b'}
  _write_zip(path, files)

  entries = list(ars.ArchiveStreamer(path))
  names = {e.name for e in entries}
  assert names == {'a.txt', 'b.txt'}
  data_map = {e.name: e.data for e in entries}
  assert data_map['a.txt'] == b'hello a'
  assert data_map['b.txt'] == b'hello b'


def test_archive_streamer_tar(tmp_path):
  """ArchiveStreamer yields entries from a tar archive."""
  path = os.path.join(tmp_path, 'data.tar')
  files = {'x.txt': b'data x', 'y.txt': b'data y'}
  _write_tar(path, files)

  entries = list(ars.ArchiveStreamer(path))
  names = {e.name for e in entries}
  assert names == {'x.txt', 'y.txt'}
  data_map = {e.name: e.data for e in entries}
  assert data_map['x.txt'] == b'data x'


def test_archive_streamer_msgpack(tmp_path):
  """ArchiveStreamer yields UID.ENTITY entries per record field."""
  path = os.path.join(tmp_path, 'data.msgpack')
  records = [{'id': 1, 'val': 'one'}, {'id': 2, 'val': 'two'}]
  _write_msgpack(path, records)

  entries = list(ars.ArchiveStreamer(path))
  assert len(entries) == 4  # 2 records x 2 fields each
  suffixes = {e.name.rsplit('_', 1)[1] for e in entries}
  assert suffixes == {'0.id', '0.val', '1.id', '1.val'}
  data_map = {e.name.rsplit('_', 1)[1]: e.data for e in entries}
  assert data_map['0.id'] == 1
  assert data_map['0.val'] == 'one'
  assert data_map['1.id'] == 2
  assert data_map['1.val'] == 'two'


def test_archive_streamer_tar_gz(tmp_path):
  """ArchiveStreamer yields entries from a compressed tar archive."""
  path = os.path.join(tmp_path, 'data.tar.gz')
  files = {'x.txt': b'data x', 'y.txt': b'data y'}
  _write_tar(path, files, mode='w:gz')

  entries = list(ars.ArchiveStreamer(path))
  names = {e.name for e in entries}
  assert names == {'x.txt', 'y.txt'}
  data_map = {e.name: e.data for e in entries}
  assert data_map['x.txt'] == b'data x'
  assert data_map['y.txt'] == b'data y'


def test_archive_streamer_empty_zip(tmp_path):
  """ArchiveStreamer on an empty zip yields no entries."""
  path = os.path.join(tmp_path, 'empty.zip')
  _write_zip(path, {})

  entries = list(ars.ArchiveStreamer(path))
  assert entries == []
