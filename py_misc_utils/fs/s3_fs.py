import collections
import functools
import io
import os
import stat as st
import tempfile
import urllib.parse as uparse

import boto3

from .. import alog as alog
from .. import assert_checks as tas
from .. import cached_file as chf
from .. import fs_base as fsb
from .. import object_cache as objc
from .. import osfd as osfd
from .. import utils as ut
from .. import writeback_file as wbf


# https://boto3.amazonaws.com/v1/documentation/api/1.35.9/reference/services/s3.html


_Credentials = collections.namedtuple(
  'Credentials',
  'access_key, secret_key, session_token',
  defaults=(None,),
)


def _get_credentials(user=None):
  if user:
    cfg_path = os.path.join(os.getenv('HOME', '.'), '.aws.conf')
    if not os.path.exists(cfg_path):
      alog.xraise(RuntimeError,
                  f'No configuration file "{cfg_path}" found to lookup credentials ' \
                  f'for user "{user}"')

    cfg = ut.load_config(cfg_file=cfg_path)
    users_cfg = cfg.get('users')
    if users_cfg is None:
      alog.xraise(RuntimeError, f'Missing "users" entry in configuration file "{cfg_path}"')

    user_cfg = users_cfg.get(user)
    if user_cfg is None:
      alog.xraise(RuntimeError, f'Missing "{user}" entry in configuration file "{cfg_path}"')

    return _Credentials(
      access_key=user_cfg.get('access_key'),
      secret_key=user_cfg.get('secret_key'),
      session_token=user_cfg.get('session_token'))
  else:
    return _Credentials(
      access_key=os.getenv('AWS_ACCESS_KEY_ID'),
      secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
      session_token=os.getenv('AWS_SESSION_TOKEN'))


def _create_session(creds):
  return boto3.session.Session(
    aws_access_key_id=creds.access_key,
    aws_secret_access_key=creds.secret_key,
    aws_session_token=creds.session_token,
  )


def _create_client(creds):
  return boto3.client(
    's3',
    aws_access_key_id=creds.access_key,
    aws_secret_access_key=creds.secret_key,
    aws_session_token=creds.session_token,
  )


def _make_dentry(resp, path, base_path=None):
  etag = resp.get('ETag', '').strip('"\'')
  etag = etag or None

  size = resp.get('ContentLength')
  if size is None:
    size = resp.get('ObjectSize')
    if size is None:
      size = resp.get('Size')

  mtime = resp.get('LastModified')
  if mtime is not None:
    mtime = mtime.timestamp()

  if base_path is not None and base_path != path:
    if base_path and not base_path.endswith('/'):
      base_path = base_path + '/'
    if not path.startswith(base_path):
      return
    name = path[len(base_path):]
    spos = name.find('/')
    if spos > 0:
      name = name[: spos]
      path = base_path + name
      size, etag, mode = 0, None, st.S_IFDIR
    else:
      mode = st.S_IFREG
  else:
    name = os.path.basename(path)
    mode = st.S_IFREG

  return fsb.DirEntry(name=name,
                      path=path,
                      etag=etag,
                      st_mode=mode,
                      st_size=size,
                      st_ctime=mtime,
                      st_mtime=mtime)


def _read_object(client, bucket, path, rdrange=None):
  if rdrange is not None:
    rdrange = f'bytes={rdrange[0]}-{rdrange[1] - 1}'

  response = client.get_object(
    Bucket=bucket,
    Key=path,
    Range=rdrange)
  stream = response.pop('Body', None)
  if stream is None:
    alog.xraise(RuntimeError, f'Error reading {bucket}:{path} object: {response}')

  return stream, _make_dentry(response, path)


def _stat_object(client, bucket, path):
  response = client.get_object_attributes(
    Bucket=bucket,
    Key=path,
    ObjectAttributes=['ObjectSize', 'ETag'],
  )

  return _make_dentry(response, path)


def _norm_path(path):
  if path:
    if path == '/':
      path = ''
    else:
      path = path + '/' if not path.endswith('/') else path

  return path


def _list_objects(client, bucket, path, flat=True):
  kwargs = dict()
  while True:
    response = client.list_objects_v2(Bucket=bucket,
                                      Prefix=path,
                                      **kwargs)

    objects = response.get('Contents', ())
    for obj in objects:
      dentry = _make_dentry(obj, obj['Key'], base_path=None if flat else path)
      if dentry is not None:
        yield dentry

    if not response.get('IsTruncated', False):
      break

    kwargs['ContinuationToken'] = response['NextContinuationToken']


def _list(client, bucket, path):
  dentries = dict()
  for dentry in _list_objects(client, bucket, path, flat=False):
    xdentry = dentries.get(dentry.name)
    if xdentry is not None:
      dentry = dentry._replace(st_ctime=min(dentry.st_ctime, xdentry.st_ctime),
                               st_mtime=max(dentry.st_mtime, xdentry.st_mtime))

    dentries[dentry.name] = dentry

  sorted_dentries = sorted(dentries.items(), key=lambda x: (x[1].st_mode, x[0]))
  for name, dentry in sorted_dentries:
    yield dentry


def _stat(client, bucket, path):
  dentries = tuple(_list(client, bucket, path))

  if dentries:
    if len(dentries) == 1:
      return dentries[0]

    ctime = min(dentry.st_ctime for dentry in dentries)
    mtime = max(dentry.st_mtime for dentry in dentries)

    bpath = path[: -1] if path.endswith('/') else path
    name = os.path.basename(bpath)

    return fsb.DirEntry(name=name,
                        path=bpath,
                        st_mode=st.S_IFDIR,
                        st_size=0,
                        st_ctime=ctime,
                        st_mtime=mtime)


def _rmtree(client, bucket, path, ignore_errors=None):
  # List all before to prevent possible erros deriving from delete-while-listing.
  dentries = tuple(_list_objects(client, bucket, _norm_path(path)))

  for dentry in dentries:
    try:
      client.delete_object(Bucket=bucket, Key=dentry.path)
    except Exception as ex:
      alog.debug(f'Failed to remove the {bucket}:{dentry.path} object: {ex}')
      if ignore_errors in (None, False):
        raise


def _write_object(client, bucket, path, body):
  response = client.put_object(
    Bucket=bucket,
    Key=path,
    Body=body,
  )


class CacheHandler(objc.Handler):

  def __init__(self, *args, **kwargs):
    super().__init__()
    self._args = args
    self._kwargs = kwargs

  def create(self):
    return _create_client(*self._args, **self._kwargs)

  def is_alive(self, obj):
    return True

  def close(self, obj):
    pass

  def max_age(self):
    return 60


class S3Reader:

  def __init__(self, client, bucket, path, sres):
    self._client = client
    self._bucket = bucket
    self._path = path
    self._sres = sres

  @classmethod
  def tag(cls, sres):
    return sres.etag or chf.make_tag(size=sres.st_size, mtime=sres.st_mtime)

  def support_blocks(self):
    return True

  def read_block(self, bpath, offset, size):
    if offset != chf.CachedBlockFile.WHOLE_OFFSET:
      size = min(size, self._sres.st_size - offset)

      stream, _ = _read_object(self._client, self._bucket, self._path,
                               rdrange=(offset, offset + size))

      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        for data in stream.iter_chunks():
          os.write(wfd, data)
    else:
      stream, _ = _read_object(self._client, self._bucket, self._path)

      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        for data in stream.iter_chunks():
          os.write(wfd, data)

    return os.path.getsize(bpath)


class S3Fs(fsb.FsBase):

  ID = 's3'
  IDS = (ID,)

  def __init__(self, cache_iface=None, **kwargs):
    super().__init__(cache_iface=cache_iface, **kwargs)

  def _get_client(self, purl):
    creds = _get_credentials(user=purl.username)
    handler = CacheHandler(creds)
    name = ('S3FS', purl.username or '$local')

    return objc.cache().get(name, handler)

  def _parse_url(self, url):
    purl = uparse.urlparse(url)
    purl = purl._replace(path=purl.path.lstrip('/'))
    client = self._get_client(purl)

    return client, purl

  def _make_reader(self, client, purl):
    sres = _stat_object(client, purl.hostname, purl.path)

    tag = S3Reader.tag(sres)
    meta = chf.Meta(size=sres.st_size, mtime=sres.st_mtime, tag=tag)
    reader = S3Reader(client, purl.hostname, purl.path, sres)

    return reader, meta

  def remove(self, url):
    client, purl = self._parse_url(url)
    client.delete_object(Bucket=purl.hostname, Key=purl.path)

  def rename(self, src_url, dest_url):
    src_client, src_purl = self._parse_url(src_url)
    dest_client, dest_purl = self._parse_url(dest_url)

    tas.check_eq(src_purl.hostname, dest_purl.hostname,
                 msg=f'Source and destination URL must be on the same bucket: ' \
                 f'{src_url} vs. {dest_url}')

    src_client.copy_object(
      Bucket=src_purl.hostname,
      CopySource=dict(Bucket=src_purl.hostname, Key=src_purl.path),
      Key=dest_purl.path,
    )
    src_client.delete_object(Bucket=src_purl.hostname, Key=src_purl.path)

  def mkdir(self, url, mode=None):
    pass

  def makedirs(self, url, mode=None, exist_ok=None):
    pass

  def rmdir(self, url):
    pass

  def rmtree(self, url, ignore_errors=None):
    client, purl = self._parse_url(url)

    _rmtree(client, purl.hostname, purl.path, ignore_errors=ignore_errors)

  def stat(self, url):
    client, purl = self._parse_url(url)

    dentry = _stat(client, purl.hostname, purl.path)
    tas.check_is_not_none(FileNotFoundError, msg=f'Not found: {purl.hostname}:{purl.path}')

    return dentry

  def list(self, url):
    client, purl = self._parse_url(url)

    return _list(client, purl.hostname, purl.path)

  def open(self, url, mode, **kwargs):
    client, purl = self._parse_url(url)

    if self.read_mode(mode):
      reader, meta = self._make_reader(client, purl)
      cfile = self._cache_iface.open(url, meta, reader)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and client.exists(purl.path):
        url_file = self._download_file(url)
        self.seek_stream(mode, url_file)
      else:
        url_file = tempfile.TemporaryFile()

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def _upload_file(self, url, stream):
    stream.seek(0)
    self.put_file(url, stream)

  def _download_file(self, url):
    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      for data in self.get_file(url):
        ftmp.v.write(data)

      return ftmp.detach()

  def put_file(self, url, stream):
    client, purl = self._parse_url(url)

    _write_object(client, purl.hostname, purl.path, stream)

  def get_file(self, url):
    client, purl = self._parse_url(url)

    stream, _ = _read_object(client, purl.hostname, purl.path)
    for data in stream.iter_chunks():
      yield data

  def as_local(self, url):
    client, purl = self._parse_url(url)
    reader, meta = self._make_reader(client, purl)

    return self._cache_iface.as_local(url, meta, reader)


FILE_SYSTEMS = (S3Fs,)

