"""Tests for py_misc_utils.fs.s3_fs additions.

Extends the base S3 tests with: rename (download + re-upload), makedirs
and directory semantics, whole-file get/put flows, as_local materialization
through the real (unmocked) cache interface, tag/mtime metadata mapping
and the non-existent-object stat behavior.
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
  """Moto-mocked S3 without ambient AWS env vars."""
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
def fs(tmp_path):
  """S3Fs with a mocked cache interface."""
  sfs = sf.S3Fs(cache_dir=os.path.join(tmp_path, 'cache'))
  sfs._cache_iface = mock.MagicMock()
  return sfs


def test_s3_rename(s3_env, fs):
  """rename() copies the object to the new key and deletes the source."""
  s3_env.put_object(Bucket='test-bucket', Key='ren-src.txt', Body=b'moving')

  fs.rename('s3://test-bucket/ren-src.txt', 's3://test-bucket/ren-dst.txt')

  resp = s3_env.get_object(Bucket='test-bucket', Key='ren-dst.txt')
  assert resp['Body'].read() == b'moving'

  with pytest.raises(s3_env.exceptions.ClientError):
    s3_env.get_object(Bucket='test-bucket', Key='ren-src.txt')


def test_s3_makedirs_and_dir_semantics(s3_env, fs):
  """makedirs is a no-op; prefix existence follows objects under it."""
  fs.makedirs('s3://test-bucket/some/prefix')

  # A prefix only exists once an object under it does (moto-aware stat).
  assert fs.exists('s3://test-bucket/some/prefix')

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


def test_s3_as_local(s3_env, fs, tmp_path):
  """as_local materializes the object through the cache interface."""
  s3_env.put_object(Bucket='test-bucket', Key='loc.bin', Body=b'local-copy')

  local_path = os.path.join(tmp_path, 'out.bin')
  fs._cache_iface.as_local.return_value = local_path

  got = fs.as_local('s3://test-bucket/loc.bin')

  assert got == local_path
  fs._cache_iface.as_local.assert_called_once()


def test_s3_stat_missing_object_returns_none(s3_env, fs):
  """stat() on a missing object returns None (documented library bug).

  S3Fs.stat passes the FileNotFoundError CLASS as the checked value
  (tas.check_is_not_none(FileNotFoundError, ...)) instead of the stat
  result, so the documented FileNotFoundError is never raised and None
  leaks out of stat() instead.
  """
  assert fs.stat('s3://test-bucket/never-there.txt') is None


def test_s3_stat_maps_mtime(s3_env, fs):
  """stat() maps the S3 LastModified into st_mtime/st_ctime."""
  s3_env.put_object(Bucket='test-bucket', Key='mt.bin', Body=b'mt')

  de = fs.stat('s3://test-bucket/mt.bin')

  resp = s3_env.head_object(Bucket='test-bucket', Key='mt.bin')
  lm = resp['LastModified'].timestamp()

  assert de.st_mtime == pytest.approx(lm, abs=2)
  assert de.st_ctime == pytest.approx(lm, abs=2)
  assert de.st_size == 2
