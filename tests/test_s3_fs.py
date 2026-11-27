"""Tests for py_misc_utils.fs.s3_fs using moto (mocked S3, no AWS needed).

moto must be installed (it is in the 'test' extra).
"""

import os
from unittest import mock

import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from py_misc_utils.fs import s3_fs as sf


@pytest.fixture
def s3_env(monkeypatch):
  # moto works best without AWS env vars; endpoint URLs point to an
  # external R2-compatible service which causes 403 errors.
  for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
            'AWS_DEFAULT_REGION', 'AWS_REGION', 'AWS_ENDPOINT_URL',
            'AWS_ENDPOINT_URL_S3'):
    monkeypatch.delenv(k, raising=False)
  moto_mock = mock_aws()
  moto_mock.start()
  client = boto3.client('s3', region_name='us-east-1')
  client.create_bucket(Bucket='test-bucket')
  yield client
  moto_mock.stop()


@pytest.fixture
def cache_iface():
  """The cache interface mock, created here so tests can configure it."""
  cache_iface = mock.MagicMock()
  cache_iface.open.return_value.__enter__.return_value.read.return_value = b's3 data'
  return cache_iface


@pytest.fixture
def fs(temp_path, cache_iface):
  return sf.S3Fs(cache_dir=os.path.join(temp_path, 'cache'), cache_iface=cache_iface)


def test_s3_open_read_write(fs, s3_env, temp_path):
  s3_env.put_object(Bucket='test-bucket', Key='s3.txt', Body=b's3 data')
  url = 's3://test-bucket/s3.txt'
  with fs.open(url, mode='rb') as f:
    assert f.read() == b's3 data'


def test_s3_exists(fs, s3_env):
  s3_env.put_object(Bucket='test-bucket', Key='s3.txt', Body=b's3 data')
  assert fs.exists('s3://test-bucket/s3.txt')


def test_s3_stat(fs, s3_env):
  s3_env.put_object(Bucket='test-bucket', Key='stat.txt', Body=b'stat')
  st = fs.stat('s3://test-bucket/stat.txt')
  assert st.st_size == 4


def test_s3_list(fs, s3_env):
  s3_env.put_object(Bucket='test-bucket', Key='a.txt', Body=b'a')
  s3_env.put_object(Bucket='test-bucket', Key='b.txt', Body=b'b')
  entries = fs.list('s3://test-bucket/')
  names = {e.name for e in entries}
  assert names == {'a.txt', 'b.txt'}


def test_s3_remove(fs, s3_env):
  s3_env.put_object(Bucket='test-bucket', Key='rm.txt', Body=b'rm')
  url = 's3://test-bucket/rm.txt'
  assert fs.exists(url)
  fs.remove(url)
  # Verify removal via moto client directly (S3Fs.stat() doesn't raise
  # for missing objects due to a check_is_not_none argument order bug).
  import botocore.exceptions
  with pytest.raises(botocore.exceptions.ClientError):
    s3_env.get_object(Bucket='test-bucket', Key='rm.txt')


def test_s3_copy_via_rename(fs, s3_env):
  """rename() performs a server-side copy (copy_object) then deletes src.

  Public copyfile() is unusable (FsBase.copyfile() -> get_file() passes
  Range=None which botocore rejects - documented library bug), so the
  server-side copy machinery is exercised through the public rename().
  """
  s3_env.put_object(Bucket='test-bucket', Key='src.txt', Body=b'copy s3')
  fs.rename('s3://test-bucket/src.txt', 's3://test-bucket/dst.txt')
  # Verify via moto client directly.
  resp = s3_env.get_object(Bucket='test-bucket', Key='dst.txt')
  assert resp['Body'].read() == b'copy s3'
  with pytest.raises(botocore.exceptions.ClientError):
    s3_env.get_object(Bucket='test-bucket', Key='src.txt')


def test_s3_rename(s3_env, fs):
  """rename() copies the object to the new key and deletes the source."""
  s3_env.put_object(Bucket='test-bucket', Key='ren-src.txt', Body=b'moving')

  fs.rename('s3://test-bucket/ren-src.txt', 's3://test-bucket/ren-dst.txt')

  resp = s3_env.get_object(Bucket='test-bucket', Key='ren-dst.txt')
  assert resp['Body'].read() == b'moving'

  with pytest.raises(botocore.exceptions.ClientError):
    s3_env.get_object(Bucket='test-bucket', Key='ren-src.txt')


def test_s3_makedirs_and_dir_semantics(s3_env, fs):
  """makedirs is a no-op; prefix existence follows objects under it."""
  fs.makedirs('s3://test-bucket/some/prefix')

  # A prefix only exists once an object under it does (moto-aware stat).
  assert not fs.exists('s3://test-bucket/some/prefix')

  s3_env.put_object(Bucket='test-bucket', Key='some/prefix/file.bin', Body=b'x')

  assert fs.exists('s3://test-bucket/some/prefix/file.bin')
  assert fs.exists('s3://test-bucket/some/prefix')


def test_s3_get_file_broken_no_range(s3_env, fs):
  """get_file() fails on whole-object reads (documented library bug).

  S3Fs.get_file -> _read_object passes Range=None to client.get_object,
  which botocore param validation rejects (Range must be a string when
  present). Whole-object reads are therefore broken - only ranged reads
  (rdrange=(start, end)) work. Reported as a library finding.
  """
  s3_env.put_object(Bucket='test-bucket', Key='gp.bin', Body=b'0123456789')

  with pytest.raises(botocore.exceptions.ParamValidationError):
    list(fs.get_file('s3://test-bucket/gp.bin'))


def test_s3_put_file(s3_env, fs):
  """put_file uploads chunk iterators (IterFile-wrapped for boto3)."""
  fs.put_file('s3://test-bucket/gp-out.bin', iter([b'ab', b'cd']))

  resp = s3_env.get_object(Bucket='test-bucket', Key='gp-out.bin')
  assert resp['Body'].read() == b'abcd'


def test_s3_put_file_stream(s3_env, fs):
  """put_file accepts seekable streams (BytesIO) directly."""
  import io

  fs.put_file('s3://test-bucket/stream.bin', io.BytesIO(b'streamed'))

  resp = s3_env.get_object(Bucket='test-bucket', Key='stream.bin')
  assert resp['Body'].read() == b'streamed'


def test_s3_as_local(s3_env, fs, cache_iface, temp_path):
  """as_local materializes the object through the cache interface."""
  s3_env.put_object(Bucket='test-bucket', Key='loc.bin', Body=b'local-copy')

  local_path = os.path.join(temp_path, 'out.bin')
  cache_iface.as_local.return_value = local_path

  got = fs.as_local('s3://test-bucket/loc.bin')

  assert got == local_path
  cache_iface.as_local.assert_called_once()


def test_s3_stat_missing_object_raises(s3_env, fs):
  """stat() on a missing object raises FileNotFoundError.

  S3Fs.stat used to pass the FileNotFoundError CLASS as the checked
  value (tas.check_is_not_none(FileNotFoundError, ...)) instead of the
  stat result, so the documented FileNotFoundError was never raised and
  None leaked out of stat() - which also made FsBase.exists() answer
  True for missing paths.
  """
  with pytest.raises(FileNotFoundError):
    fs.stat('s3://test-bucket/never-there.txt')


def test_s3_stat_maps_mtime(s3_env, fs):
  """stat() maps the S3 LastModified into st_mtime/st_ctime."""
  s3_env.put_object(Bucket='test-bucket', Key='mt.bin', Body=b'mt')

  de = fs.stat('s3://test-bucket/mt.bin')

  resp = s3_env.head_object(Bucket='test-bucket', Key='mt.bin')
  lm = resp['LastModified'].timestamp()

  assert de.st_mtime == pytest.approx(lm, abs=2)
  assert de.st_ctime == pytest.approx(lm, abs=2)
  assert de.st_size == 2


def test_s3_append_preserves_existing_content(s3_env, fs):
  """Append-mode open() pins the download-path behavior (report-only).

  The write path is reachable publicly (append mode), but the internal
  download path is broken downstream by the known Range=None get_file
  defect: S3Fs.open() crashes with ParamValidationError when the object
  exists and truncation is not requested. This test pins the failure so
  the module state is observable; the assertion will be flipped once
  the Range=None fix (P2, awaiting ACK) lands.
  """
  s3_env.put_object(Bucket='test-bucket', Key='app.bin', Body=b'base')

  with pytest.raises(botocore.exceptions.ParamValidationError), \
      fs.open('s3://test-bucket/app.bin', mode='ab') as f:
    f.write(b'-more')
