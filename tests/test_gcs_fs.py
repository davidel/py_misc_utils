"""Tests for py_misc_utils.fs.gcs_fs using monkeypatched internals.

No real GCP access or credentials are required.
"""

from unittest import mock

import pytest

from py_misc_utils.fs import gcs_fs as gf


@pytest.fixture
def fs(tmp_path):
  return gf.GcsFs(cache_dir=str(tmp_path / 'cache'))


@pytest.fixture
def mock_gcsfs():
  """Return a mock top-level GcsFs with stat/list/remove/copy."""
  g = mock.MagicMock()
  g.stat.return_value = mock.MagicMock(st_size=42, st_mtime=1.0)
  g.listdir.return_value = ['a.txt', 'b.txt']
  return g


def test_gcs_open_read(fs, tmp_path, monkeypatch, mock_gcsfs):
  monkeypatch.setattr(gf.GcsFs, '_get_fs', lambda self, bucket: mock_gcsfs)
  fs._cache_iface = mock.MagicMock()
  fs._cache_iface.open.return_value.__enter__.return_value.read.return_value = b'gcs data'
  with fs.open('gs://test-bucket/gcs.txt', mode='rb') as f:
    data = f.read()
    assert data == b'gcs data'


def test_gcs_exists(fs, monkeypatch, mock_gcsfs):
  mock_gcsfs.stat.return_value = mock.MagicMock()
  monkeypatch.setattr(gf.GcsFs, '_get_fs', lambda self, bucket: mock_gcsfs)
  assert fs.exists('gs://test-bucket/gcs.txt')  # stat returns truthy
  mock_gcsfs.stat.side_effect = FileNotFoundError('not found')
  assert fs.exists('gs://test-bucket/missing.txt') is False


def test_gcs_stat(fs, monkeypatch, mock_gcsfs):
  mock_gcsfs.stat.return_value = mock.MagicMock(st_size=42)
  monkeypatch.setattr(gf.GcsFs, '_get_fs', lambda self, bucket: mock_gcsfs)
  st = fs.stat('gs://test-bucket/stat.txt')
  assert st.st_size == 42


def test_gcs_list(fs, monkeypatch, mock_gcsfs):
  monkeypatch.setattr(gf.GcsFs, '_get_fs', lambda self, bucket: mock_gcsfs)
  entries = fs.list('gs://test-bucket/')
  assert set(entries) == {'a.txt', 'b.txt'}


def test_gcs_remove(fs, monkeypatch, mock_gcsfs):
  monkeypatch.setattr(gf.GcsFs, '_get_fs', lambda self, bucket: mock_gcsfs)
  fs.remove('gs://test-bucket/rm.txt')
  mock_gcsfs.remove.assert_called_once()


def test_gcs_copy(fs, monkeypatch, mock_gcsfs):
  monkeypatch.setattr(gf.GcsFs, '_get_fs', lambda self, bucket: mock_gcsfs)
  fs.copyfile('gs://test-bucket/src.txt', fs, 'gs://test-bucket/dst.txt')
  # copyfile inherits FsBase.copyfile() -> get_file() + put_file(), not _copy().
  # self.get_file() -> mock_gcsfs.download(); self.put_file() -> mock_gcsfs.upload().
  mock_gcsfs.download.assert_called_once_with('src.txt')
  mock_gcsfs.upload.assert_called_once()
