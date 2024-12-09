import collections
import os
import stat as st

import google.cloud.storage as gcs

from . import alog
from . import fs_base as fsb


# https://cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
# https://cloud.google.com/python/docs/reference/storage/1.44.0/client
# https://cloud.google.com/python/docs/reference/storage/1.44.0/blobs#google.cloud.storage.blob.Blob

class GcsFs:

  def __init__(self, bucket):
    self.bucket = bucket
    self._client = gcs.Client()

  def _blob_stat(self, blob, base_path=None):
    name = blob.name
    if base_path is not None:
      if not name.startswith(base_path):
        return
      name = name[len(base_path):]
      spos = name.find('/')
      if spos > 0:
        name = name[: spos]
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
    if path:
      if path == '/':
        path = ''
      else:
        path = path + '/' if not path.endswith('/') else path

    return path

  def listdir(self, path):
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
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    return blob.open(mode)

  def upload(self, path, source):
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    with blob.open('wb') as fd:
      for data in source:
        fd.write(data)

  def download(self, path, chunk_size=32 * 1024**2):
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
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    return blob.download_as_bytes(start=offset, end=offset + size - 1, raw_download=True)

  def exists(self, path):
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)

    return blob.exists()

  def stat(self, path):
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
      bpath = path[: -1] if path.endswith('/') else path
      name = os.path.basename(bpath)

      return fsb.DirEntry(name=name,
                          path=bpath,
                          st_mode=st.S_IFDIR,
                          st_size=0,
                          st_ctime=ctime,
                          st_mtime=mtime)

  def remove(self, path):
    bucket = self._client.bucket(self.bucket)
    blob = bucket.blob(path)
    blob.delete()

  def rename(self, src_path, dest_path):
    bucket = self._client.bucket(self.bucket)
    src_blob = bucket.blob(src_path)

    bucket.copy_blob(src_blob, bucket, dest_path)
    bucket.delete_blob(src_path)

  def rmtree(self, path, ignore_errors=None):
    npath = self._norm_path(path)

    for blob in self._client.list_blobs(self.bucket, prefix=npath):
      try:
        blob.delete()
      except Exception as ex:
        alog.debug(f'Failed to remove "{blob.name}" from "{self.bucket}": {ex}')
        if ignore_errors in (None, False):
          raise

  def copy(self, src_path, dest_path):
    bucket = self._client.bucket(self.bucket)
    src_blob = bucket.blob(src_path)

    bucket.copy_blob(src_blob, bucket, dest_path)

