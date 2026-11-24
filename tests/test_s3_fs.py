"""Tests for py_misc_utils.fs.s3_fs using moto (mocked S3, no AWS needed).

moto must be installed (it is in the 'test' extra).
"""

import boto3
import pytest
from moto import mock_aws

from py_misc_utils.fs import s3_fs as sf


@pytest.fixture
def s3_env(monkeypatch):
  monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
  monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
  monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
  with mock_aws():
    client = boto3.client('s3', region_name='us-east-1')
    client.create_bucket(
        Bucket='test-bucket',
        CreateBucketConfiguration={'LocationConstraint': 'us-east-1'})
    yield client


@pytest.fixture
def fs(tmp_path):
  return sf.S3Fs(cache_dir=str(tmp_path / 'cache'))


def test_s3_open_read_write(fs, s3_env, tmp_path):
  s3_env.put_object(Bucket='test-bucket', Key='s3.txt', Body=b's3 data')
  url = 's3://test-bucket/s3.txt'
  with fs.open(url, mode='rb') as f:
    assert f.read() == b's3 data'


def test_s3_exists(fs, s3_env):
  assert fs.exists('s3://test-bucket/s3.txt')
  assert not fs.exists('s3://test-bucket/missing.txt')


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
  assert not fs.exists(url)


def test_s3_copy(fs, s3_env):
  s3_env.put_object(Bucket='test-bucket', Key='src.txt', Body=b'copy s3')
  fs.copyfile('s3://test-bucket/src.txt', fs, 's3://test-bucket/dst.txt')
  with fs.open('s3://test-bucket/dst.txt', mode='rb') as f:
    assert f.read() == b'copy s3'
