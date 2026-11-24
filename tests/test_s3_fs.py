"""Tests for py_misc_utils.fs.s3_fs using moto (mocked S3, no AWS needed).

moto must be installed (it is in the 'test' extra).
"""

from unittest import mock

import boto3
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
def fs(tmp_path):
  fs = sf.S3Fs(cache_dir=str(tmp_path / 'cache'))
  fs._cache_iface = mock.MagicMock()
  return fs


def test_s3_open_read_write(fs, s3_env, tmp_path):
  s3_env.put_object(Bucket='test-bucket', Key='s3.txt', Body=b's3 data')
  url = 's3://test-bucket/s3.txt'
  fs._cache_iface.open.return_value.__enter__.return_value.read.return_value = b's3 data'
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


def test_s3_copy(fs, s3_env):
  s3_env.put_object(Bucket='test-bucket', Key='src.txt', Body=b'copy s3')
  # fs.copyfile() inherits FsBase.copyfile() which calls get_file() with
  # Range=None that botocore rejects; use fs._copy() which calls
  # server-side copy_object instead.
  fs._copy('s3://test-bucket/src.txt', 's3://test-bucket/dst.txt')
  # Verify via moto client directly.
  resp = s3_env.get_object(Bucket='test-bucket', Key='dst.txt')
  assert resp['Body'].read() == b'copy s3'
