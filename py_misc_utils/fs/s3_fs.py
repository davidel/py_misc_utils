"""S3 FileSystem implementation using boto3.

This module provides S3Fs, a FileSystem backend for s3:// URLs backed by
the boto3 S3 client. Credentials are resolved from a user entry of the
~/.aws.conf configuration file or from the AWS_* environment variables,
and clients are pooled per user through the object cache. Reads go through
the block-cache machinery (S3 supports ranged reads) while writes are
staged locally and uploaded on close.
"""

import collections
import functools
import io
import os
import stat as st
import tempfile
import urllib.parse as uparse

import boto3

from .. import alog
from .. import assert_checks as tas
from .. import cached_file as chf
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import iter_file as itf
from .. import object_cache as objc
from .. import osfd
from .. import utils as ut
from .. import writeback_file as wbf

# https://boto3.amazonaws.com/v1/documentation/api/1.35.9/reference/services/s3.html

# Credentials to access S3, resolved from the environment or ~/.aws.conf.
_Credentials = collections.namedtuple(
    'Credentials',
    'access_key, secret_key, session_token',
    defaults=(None,),
)


def _get_credentials(user=None):
  """Resolve the S3 credentials.

  Args:
    user: Name of a user entry of the 'users' section in the ~/.aws.conf
      configuration file. When missing, the credentials are taken from
      the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN
      environment variables - returning empty credentials when they are
      not set (the boto3 machinery resolves them in that case).

  Returns:
    The resolved _Credentials tuple.

  Raises:
    RuntimeError: When the user is specified and the configuration file or
      its users/user entries are missing.
  """
  if user:
    cfg_path = os.path.join(fsu.home(), '.aws.conf')
    if not os.path.exists(cfg_path):
      alog.xraise(RuntimeError,
                  f'No configuration file "{cfg_path}" found to lookup credentials ' \
                  f'for user "{user}"')

    cfg = ut.load_config(cfg_path)
    users_cfg = cfg.get('users')
    if users_cfg is None:
      alog.xraise(RuntimeError,
                  f'Missing "users" entry in configuration file "{cfg_path}"')

    user_cfg = users_cfg.get(user)
    if user_cfg is None:
      alog.xraise(RuntimeError,
                  f'Missing "{user}" entry in configuration file "{cfg_path}"')

    return _Credentials(access_key=user_cfg.get('access_key'),
                        secret_key=user_cfg.get('secret_key'),
                        session_token=user_cfg.get('session_token'))
  else:
    return _Credentials(access_key=os.getenv('AWS_ACCESS_KEY_ID'),
                        secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                        session_token=os.getenv('AWS_SESSION_TOKEN'))


def _create_session(creds):
  """Create a boto3 session from resolved credentials.

  Args:
    creds: _Credentials tuple with the access key, secret key and session
      token.

  Returns:
    A boto3.session.Session configured with the credentials.
  """
  return boto3.session.Session(
      aws_access_key_id=creds.access_key,
      aws_secret_access_key=creds.secret_key,
      aws_session_token=creds.session_token,
  )


def _create_client(creds):
  """Create an S3 client from resolved credentials.

  Args:
    creds: _Credentials tuple with the access key, secret key and session
      token.

  Returns:
    A boto3 S3 client configured with the credentials.
  """
  return boto3.client(
      's3',
      aws_access_key_id=creds.access_key,
      aws_secret_access_key=creds.secret_key,
      aws_session_token=creds.session_token,
  )


def _make_dentry(resp, path, base_path=None):
  """Build a DirEntry from an S3 API response.

  The path is used to derive the entry name, and when a base_path is
  specified the path is expected to be under it: entries below the
  base-path prefix are folded into pseudo-directories, since S3 objects
  are flat.

  Args:
    resp: Dictionary with the fields of an S3 object listing or attribute
      response, such as ETag, ContentLength and LastModified.
    path: Object key of the entry.
    base_path: Prefix the entry must be relative to; entries outside the
      prefix are filtered out.

  Returns:
    The DirEntry describing the object, or None when the path is outside
    the base-path prefix.
  """
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
      name = name[:spos]
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
  """Read an object body, optionally within a byte range.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket holding the object.
    path: Key of the object to read.
    rdrange: Optional (start, end) byte range to read.

  Returns:
    A tuple with the streaming body of the object and the DirEntry built
    from the response metadata.

  Raises:
    RuntimeError: When the response does not include the object body.
  """
  kwargs = dict(Bucket=bucket, Key=path)
  if rdrange is not None:
    kwargs['Range'] = f'bytes={rdrange[0]}-{rdrange[1] - 1}'

  response = client.get_object(**kwargs)
  stream = response.pop('Body', None)
  if stream is None:
    alog.xraise(RuntimeError, f'Error reading {bucket}:{path} object: {response}')

  return stream, _make_dentry(response, path)


def _stat_object(client, bucket, path):
  """Stat an object using the get_object_attributes API.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket holding the object.
    path: Key of the object to stat.

  Returns:
    The DirEntry describing the object, or None if it does not exist.
  """
  response = client.get_object_attributes(
      Bucket=bucket,
      Key=path,
      ObjectAttributes=['ObjectSize', 'ETag'],
  )

  return _make_dentry(response, path)


def _norm_path(path):
  """Normalize a path into a directory prefix used for listings.

  Args:
    path: Path to normalize.

  Returns:
    The path with a trailing slash added, or the empty string for the
    root path '/'.
  """
  if path:
    if path == '/':
      path = ''
    else:
      path = path + '/' if not path.endswith('/') else path

  return path


def _list_objects(client, bucket, path, flat=True):
  """Enumerate the objects under a path prefix, handling pagination.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket to list.
    path: Prefix of the objects to enumerate.
    flat: When True entries are returned with their full key; when False
      paths below the prefix are folded into pseudo-directory entries.

  Yields:
    A DirEntry for every object under the prefix.
  """
  kwargs = dict()
  while True:
    response = client.list_objects_v2(Bucket=bucket, Prefix=path, **kwargs)

    objects = response.get('Contents', ())
    for obj in objects:
      dentry = _make_dentry(obj, obj['Key'], base_path=None if flat else path)
      if dentry is not None:
        yield dentry

    if not response.get('IsTruncated', False):
      break

    kwargs['ContinuationToken'] = response['NextContinuationToken']


def _list(client, bucket, path):
  """List the entries under a path, merging directory pseudo-entries.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket to list.
    path: Prefix of the entries to enumerate.

  Yields:
    A DirEntry for every entry under the path, sorted with directories
    first and names in alphabetical order. Duplicate directory entries
    are merged keeping the earliest ctime and the latest mtime.
  """
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
  """Stat a path by listing its entries.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket holding the path.
    path: Path to stat.

  Returns:
    The single DirEntry of the path when there is only one, a directory
    DirEntry aggregating the entries when there are more, or None when
    the path does not exist.
  """
  dentries = tuple(_list(client, bucket, path))

  if dentries:
    if len(dentries) == 1:
      return dentries[0]

    ctime = min(dentry.st_ctime for dentry in dentries)
    mtime = max(dentry.st_mtime for dentry in dentries)

    bpath = path.removesuffix('/')
    name = os.path.basename(bpath)

    return fsb.DirEntry(name=name,
                        path=bpath,
                        st_mode=st.S_IFDIR,
                        st_size=0,
                        st_ctime=ctime,
                        st_mtime=mtime)


def _rmtree(client, bucket, path, ignore_errors=None):
  """Recursively remove all the objects under a path.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket holding the path.
    path: Prefix of the objects to remove.
    ignore_errors: When True errors raised while removing the objects are
      logged instead of propagating.
  """
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
  """Upload data into an object, adapting iterators for the API.

  Args:
    client: boto3 S3 client.
    bucket: Name of the bucket holding the object.
    path: Key of the object to write.
    body: Data to upload, as bytes, a seekable stream or an iterator of
      chunks; plain iterators are wrapped into an IterFile so that the
      boto3 machinery can stream them.
    """
  if isinstance(body, collections.abc.Iterator) and not hasattr(body, 'seek'):
    body = itf.IterFile(body)

  client.put_object(
      Bucket=bucket,
      Key=path,
      Body=body,
  )


class CacheHandler(objc.Handler):
  """Object-cache handler creating pooled boto3 S3 clients."""

  def __init__(self, *args, **kwargs):
    """Initialize the handler with the client creation arguments.

    Args:
      *args: Positional arguments forwarded to _create_client(), such as
        the resolved credentials.
      **kwargs: Keyword arguments forwarded to _create_client().
    """
    super().__init__()
    # Credentials used to create the S3 client.
    self._args = args
    self._kwargs = kwargs

  def create(self):
    """Create a new S3 client.

    Returns:
      A boto3 S3 client configured with the handler credentials.
    """
    return _create_client(*self._args, **self._kwargs)


class S3Reader:
  """Reader fetching data from S3 using ranged reads."""

  def __init__(self, client, bucket, path, sres):
    """Initialize the reader.

    Args:
      client: boto3 S3 client used for the reads.
      bucket: Name of the bucket holding the object.
      path: Key of the object to read.
      sres: DirEntry of the object.
    """
    # S3 client, bucket and object key of the file to read and its stat
    # result.
    self._client = client
    self._bucket = bucket
    self._path = path
    self._sres = sres

  @classmethod
  def tag(cls, sres):
    """Build a cache tag from an S3 stat result.

    Args:
      sres: DirEntry of the object, as returned by _stat().

    Returns:
      The object ETag, or a tag built from size and modification time.
    """
    return sres.etag or chf.make_tag(size=sres.st_size, mtime=sres.st_mtime)

  def support_blocks(self):
    """Return True since S3 supports ranged reads."""
    return True

  def read_block(self, bpath, offset, size):
    """Read data into a local cache file.

    Args:
      bpath: Local path of the cache file to write the contents to.
      offset: Offset in the object where reading starts; WHOLE_OFFSET
        streams the whole object.
      size: Number of bytes to read.

    Returns:
      The number of bytes written into the cache file.
    """
    if offset != chf.CachedBlockFile.WHOLE_OFFSET:
      size = min(size, self._sres.st_size - offset)

      stream, _ = _read_object(self._client,
                               self._bucket,
                               self._path,
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
  """FileSystem implementation for s3:// URLs.

  S3 clients are pooled per resolved credentials through the object cache,
  while reads and writes go through the block-cache and writeback-file
  machinery. Since S3 objects are flat, listings fold paths below the
  query prefix into pseudo-directory entries.

  The credentials are resolved from the user name of the URL - when
  present - looked up as a user entry of the ~/.aws.conf configuration
  file (see _get_credentials); URLs without a user fall back to the
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN
  environment variables.

  See py_misc_utils.fs for the shared backend contract.

  Example:
    from py_misc_utils import cached_file as chf
    from py_misc_utils.fs import s3_fs as sf

    iface = chf.CacheInterface('/tmp/gfs-cache')
    # s3://prod@my-bucket resolves the 'prod' user of ~/.aws.conf.
    fs = sf.S3Fs(cache_dir='/tmp/gfs-cache', cache_iface=iface)

    with fs.open('s3://my-bucket/data/file.bin', mode='rb') as f:
      header = f.read(512)
  """

  ID = 's3'
  IDS = (ID,)

  def __init__(self, cache_iface=None, **kwargs):
    """Initialize the backend.

    Args:
      cache_iface: Optional cache interface customizing the caching of
        the remote contents.
      **kwargs: Extra keyword arguments forwarded to FsBase.
    """
    super().__init__(cache_iface=cache_iface, **kwargs)

  def _get_client(self, purl):
    """Get a pooled S3 client.

    Args:
      purl: Parsed s3:// URL whose user name selects the credentials, as
        per _get_credentials().

    Returns:
      The S3 client from the object cache, shared per user name.
    """
    creds = _get_credentials(user=purl.username)
    handler = CacheHandler(creds)
    name = ('S3FS', purl.username or '$local')

    return objc.cache().get(name, handler)

  def _parse_url(self, url):
    """Parse an s3:// URL and return its S3 client.

    Args:
      url: s3:// URL to parse.

    Returns:
      A tuple with the S3 client and the parsed URL with the leading
      slash of the path removed.
    """
    purl = uparse.urlparse(url)
    purl = purl._replace(path=purl.path.lstrip('/'))
    client = self._get_client(purl)

    return client, purl

  def _make_reader(self, client, purl):
    """Create a reader and metadata for the remote object.

    Args:
      client: boto3 S3 client.
      purl: Parsed s3:// URL.

    Returns:
      A tuple with the S3Reader and the cache Meta describing the object.
    """
    sres = _stat_object(client, purl.hostname, purl.path)

    tag = S3Reader.tag(sres)
    meta = chf.Meta(size=sres.st_size, mtime=sres.st_mtime, tag=tag)
    reader = S3Reader(client, purl.hostname, purl.path, sres)

    return reader, meta

  def _copy(self, src_url, dest_url):
    """Server-side copy an object within the same bucket.

    Args:
      src_url: s3:// URL of the source object.
      dest_url: s3:// URL of the destination object.
    """
    src_client, src_purl = self._parse_url(src_url)
    _, dest_purl = self._parse_url(dest_url)

    tas.check_eq(src_purl.hostname, dest_purl.hostname,
                 msg=f'Source and destination URL must be on the same bucket: ' \
                 f'{src_url} vs. {dest_url}')

    src_client.copy_object(
        Bucket=src_purl.hostname,
        CopySource=dict(Bucket=src_purl.hostname, Key=src_purl.path),
        Key=dest_purl.path,
    )

  def remove(self, url):
    """Remove an object.

    Args:
      url: s3:// URL of the object to remove.
    """
    client, purl = self._parse_url(url)
    client.delete_object(Bucket=purl.hostname, Key=purl.path)

  def rename(self, src_url, dest_url):
    """Rename an object with a copy followed by a delete.

    Args:
      src_url: s3:// URL of the source object.
      dest_url: s3:// URL of the destination object.
    """
    self._copy(src_url, dest_url)

    src_client, src_purl = self._parse_url(src_url)
    src_client.delete_object(Bucket=src_purl.hostname, Key=src_purl.path)

  def mkdir(self, url, mode=None):
    """No-op since S3 has no real directories."""

  def makedirs(self, url, mode=None, exist_ok=None):
    """No-op since S3 has no real directories."""

  def rmdir(self, url):
    """No-op since S3 has no real directories."""

  def rmtree(self, url, ignore_errors=None):
    """Recursively remove all the objects under a URL.

    Args:
      url: s3:// URL of the prefix to remove.
      ignore_errors: When True errors raised while removing the objects
        are logged instead of propagating.
    """
    client, purl = self._parse_url(url)

    _rmtree(client, purl.hostname, purl.path, ignore_errors=ignore_errors)

  def stat(self, url):
    """Stat a path.

    Args:
      url: s3:// URL to stat.

    Returns:
      The DirEntry of the path.

    Raises:
      FileNotFoundError: If the path does not exist.
    """
    client, purl = self._parse_url(url)

    dentry = _stat(client, purl.hostname, purl.path)
    tas.check_is_not_none(dentry,
                          exception=FileNotFoundError,
                          msg=f'Not found: {purl.hostname}:{purl.path}')

    return dentry

  def list(self, url):
    """List the entries under a URL.

    Args:
      url: s3:// URL of the prefix to enumerate.

    Returns:
      A list of DirEntry objects for the entries under the prefix.
    """
    client, purl = self._parse_url(url)

    return _list(client, purl.hostname, purl.path)

  def open(self, url, mode, **kwargs):
    """Open a URL for reading or writing.

    Read modes are served through the block-cache interface, while write
    modes download the existing contents when truncation is not requested
    and wrap the stream into a WritebackFile uploading on close.

    Args:
      url: s3:// URL to open.
      mode: Open mode string.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      A file-like object, wrapped into a TextIOWrapper in text modes.
    """
    client, purl = self._parse_url(url)

    if self.read_mode(mode):
      reader, meta = self._make_reader(client, purl)
      cfile = self._cache_iface.open(url, meta, reader, **kwargs)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and self.exists(url):
        url_file = self._download_file(url)
        self.seek_stream(mode, url_file)
      else:
        # The WritebackFile owns the temp file's lifetime.
        url_file = tempfile.TemporaryFile()  # noqa: SIM115

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def _upload_file(self, url, stream):
    """Rewind a stream and upload its contents to the given URL.

    Args:
      url: Destination s3:// URL of the upload.
      stream: Seekable file-like object with the contents to upload.
    """
    stream.seek(0)
    self.put_file(url, stream)

  def _download_file(self, url):
    """Download the contents of a URL into a temporary file.

    Args:
      url: s3:// URL to download.

    Returns:
      A seekable temporary file with the downloaded contents.
    """
    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      for data in self.get_file(url):
        ftmp.v.write(data)

      return ftmp.detach()

  def put_file(self, url, stream):
    """Upload data to a URL.

    Args:
      url: Destination s3:// URL of the upload.
      stream: Data to upload, as a stream or an iterable of chunks, as
        per _write_object().
    """
    client, purl = self._parse_url(url)

    _write_object(client, purl.hostname, purl.path, stream)

  def get_file(self, url):
    """Read the contents of a URL as chunks.

    Args:
      url: s3:// URL to read.

    Yields:
      The file contents as bytes chunks.
    """
    client, purl = self._parse_url(url)

    stream, _ = _read_object(client, purl.hostname, purl.path)

    yield from stream.iter_chunks()

  def as_local(self, url, **kwargs):
    """Return a local file with the contents of a URL.

    Args:
      url: s3:// URL of the file to access.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      The path of the local file, downloaded through the cache interface.
    """
    client, purl = self._parse_url(url)
    reader, meta = self._make_reader(client, purl)

    return self._cache_iface.as_local(url, meta, reader, **kwargs)

  def link(self, src_url, dest_url):
    """Copy an object, since S3 has no hard links.

    Args:
      src_url: s3:// URL of the source object.
      dest_url: s3:// URL of the destination object.
    """
    self._copy(src_url, dest_url)

  def symlink(self, src_url, dest_url):
    """Copy an object, since S3 has no symbolic links.

    Args:
      src_url: s3:// URL of the source object.
      dest_url: s3:// URL of the destination object.
    """
    self.link(src_url, dest_url)


FILE_SYSTEMS = (S3Fs,)
