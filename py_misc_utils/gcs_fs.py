"""Google Cloud Storage bucket filesystem.

This module provides the GcsFs class, a filesystem-like interface to the
objects of a Google Cloud Storage bucket: listing, streaming and partial
reads, uploads, copies and removals are exposed over plain paths.

The credentials are resolved by the google-cloud-storage machinery - from
the GOOGLE_APPLICATION_CREDENTIALS environment variable or the
environment default - and can be pointed at an emulator with the
STORAGE_EMULATOR_HOST environment variable.

Example:
  from py_misc_utils import gcs_fs

  fs = gcs_fs.GcsFs('my-bucket')

  # Upload and stream back.
  fs.upload('data/file.bin', [b'chunk1', b'chunk2'])
  data = b''.join(fs.download('data/file.bin'))

  # Partial read without downloading the whole object.
  head = fs.pread('data/file.bin', 0, 512)

  # Flat listing with directory collapsing.
  for dentry in fs.listdir('data'):
    print(dentry.name, dentry.st_size)
"""

import os
import stat as st

import google.cloud.storage as gcs

from . import alog
from . import fs_base as fsb

# https://cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
# https://cloud.google.com/python/docs/reference/storage/1.44.0/client
# https://cloud.google.com/python/docs/reference/storage/1.44.0/blobs#google.cloud.storage.blob.Blob


class GcsFs:
  """Filesystem-like access to a Google Cloud Storage bucket.

  The class provides listing, streaming read/write, partial reads and
  object management operations over the objects of a bucket, exposing
  them as directory entries and file paths.

  See py_misc_utils.fs.gcs_fs for the gs:// URL backend built on top of
  this class, and py_misc_utils.fs for the shared backend contract.
  """

  def __init__(self, bucket):
    """Initialize the GCS filesystem access.

    Args:
      bucket: Name of the GCS bucket to operate on.
    """
    self.bucket = bucket
    self._client = gcs.Client()

  def _blob_stat(self, blob, base_path=None):
    """Convert a GCS blob into a filesystem directory entry.

    Args:
      blob: Blob to describe.
      base_path: Optional prefix the blob is listed under; when set the
        entry describes the path relative to it, and blobs belonging to
        other prefixes yield no entry.

    Returns:
      A DirEntry describing the blob -- as a directory when the blob is
      a shared prefix of others -- or None when the blob is outside the
      base path.
    """
    name = blob.name
    if base_path is not None:
      if not name.startswith(base_path):
        return
      name = name[len(base_path):]
      spos = name.find('/')
      if spos > 0:
        name = name[:spos]
        mode = st.S_IFDIR
        size, etag = 0, None
      else:
        mode = st.S_IFREG
        size, etag = blob.size, blob.etag

      path = base_path + name
    else:
      spos = name.rfind('/')
      if spos >= 0:
        name = name[spos + 1:]
      mode = st.S_IFREG
      path = blob.name
      size, etag = blob.size, blob.etag

    return fsb.DirEntry(name=name,
                        path=path,
                        etag=etag,
                        st_mode=mode,
                        st_size=size,
                        st_ctime=blob.time_created.timestamp(),
                        st_mtime=blob.updated.timestamp())

  def _norm_path(self, path):
    """Normalize a path into a blob name prefix.

    Args:
      path: Path to normalize, as a blob name or directory-like path.

    Returns:
      The blob name prefix to use for listing operations, with '/' and
      empty paths normalized to the empty prefix.
    """
    if path:
      if path == '/':
        path = ''
      else:
        path = path + '/' if not path.endswith('/') else path

    return path

  def listdir(self, path):
    """List the entries of a path in the bucket.

    Blobs sharing a name prefix are collapsed into a single directory
    entry holding the aggregate creation and modification times.

    Args:
      path: Path to list; an empty or '/' path lists the bucket root.

    Yields:
      DirEntry objects sorted with directories first, by name.
    """
    npath = self._norm_path(path)

    dentries = dict()
    for blob in self._client.list_blobs(self.bucket, prefix=npath):
      if (dentry := self._blob_stat(blob, base_path=npath)) is not None:
        xdentry = dentries.get(dentry.name)
        if xdentry is not None:
          dentry = dentry._replace(st_ctime=min(dentry.st_ctime, xdentry.st_ctime),
                                   st_mtime=max(dentry.st_mtime, xdentry.st_mtime))

        dentries[dentry.name] = dentry

    sorted_dentries = sorted(dentries.items(), key=lambda x: (x[1].st_mode, x[0]))
    for name, dentry in sorted_dentries:
      yield dentry

  def open(self, path, mode='rb'):
    """Open a blob for reading or writing.

    Args:
      path: Blob path to open.
      mode: Mode to open the blob with ('rb' or 'wb').

    Returns:
      A file-like object backed by the blob.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    return blob.open(mode)

  def upload(self, path, source):
    """Upload data to a blob.

    Args:
      path: Blob path to upload to.
      source: Iterable of chunks of data to upload.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    with blob.open('wb') as fd:
      for data in source:
        fd.write(data)

  def download(self, path, chunk_size=32 * 1024**2):
    """Download a blob as a sequence of data chunks.

    Args:
      path: Blob path to download.
      chunk_size: Maximum size of each downloaded chunk.

    Yields:
      Chunks of the blob content.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    with blob.open('rb') as fd:
      while True:
        data = fd.read(chunk_size)
        if data:
          yield data
        if chunk_size > len(data):
          break

  def pread(self, path, offset, size):
    """Read a byte range of a blob without downloading the whole object.

    Args:
      path: Blob path to read from.
      offset: Start offset of the range to read.
      size: Number of bytes to read.

    Returns:
      The bytes read from the blob at the requested range.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    return blob.download_as_bytes(start=offset,
                                  end=offset + size - 1,
                                  raw_download=True)

  def exists(self, path):
    """Check whether a blob exists in the bucket.

    Args:
      path: Blob path to check.

    Returns:
      True if the blob exists, False otherwise.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    return blob.exists()

  def stat(self, path):
    """Describe a blob or a directory-like path prefix.

    Args:
      path: Blob path to describe.

    Returns:
      A DirEntry describing the blob, or -- when the path does not map
      to a single blob but matches a group of shared-prefix blobs -- a
      directory entry with aggregate times, or None when the path does
      not exist.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.get_blob(path)
    if blob is not None:
      return self._blob_stat(blob)

    ctime = mtime = None
    for de in self.listdir(path):
      if ctime is None or de.st_ctime < ctime:
        ctime = de.st_ctime
      if mtime is None or de.st_mtime > mtime:
        mtime = de.st_mtime

    if ctime is not None and mtime is not None:
      bpath = path.removesuffix('/')
      name = os.path.basename(bpath)

      return fsb.DirEntry(name=name,
                          path=bpath,
                          st_mode=st.S_IFDIR,
                          st_size=0,
                          st_ctime=ctime,
                          st_mtime=mtime)

  def remove(self, path):
    """Delete a blob from the bucket.

    Args:
      path: Blob path to remove.
    """
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)
    blob.delete()

  def rename(self, src_path, dest_path):
    """Move a blob to a new path.

    Args:
      src_path: Blob path to move.
      dest_path: Destination blob path.
    """
    bucket = self._client.bucket(self.bucket)
    src_blob = bucket.blob(src_path)

    bucket.copy_blob(src_blob, bucket, dest_path)
    bucket.delete_blob(src_path)

  def rmtree(self, path, ignore_errors=None):
    """Delete all the blobs under a path prefix.

    Args:
      path: Path prefix whose blobs are to be removed.
      ignore_errors: Whether to suppress deletion errors; True suppresses
        them, False or None (the default) re-raises the first error.
    """
    npath = self._norm_path(path)

    for blob in self._client.list_blobs(self.bucket, prefix=npath):
      try:
        blob.delete()
      except Exception as ex:
        alog.debug(f'Failed to remove "{blob.name}" from "{self.bucket}": {ex}')
        if ignore_errors in (None, False):
          raise

  def copy(self, src_path, dest_path):
    """Copy a blob to a new path.

    Args:
      src_path: Blob path to copy.
      dest_path: Destination blob path.
    """
    bucket = self._client.bucket(self.bucket)
    src_blob = bucket.blob(src_path)

    bucket.copy_blob(src_blob, bucket, dest_path)
