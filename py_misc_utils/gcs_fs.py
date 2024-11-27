import collections
import os
import stat as st

import google.cloud.storage as gcs


# https://cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
# https://cloud.google.com/python/docs/reference/storage/1.44.0/client
# https://cloud.google.com/python/docs/reference/storage/1.44.0/blobs#google.cloud.storage.blob.Blob

DirEntry = collections.namedtuple('DirEntry', 'name, path, st_mode, st_size, st_ctime, st_mtime')

class GcsFs:

  def __init__(self, bucket):
    self._bucket = bucket
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
        size = 0
      else:
        mode = st.S_IFREG
        size = blob.size

      path = base_path + name
    else:
      spos = name.rfind('/')
      if spos >= 0:
        name = name[spos + 1:]
      mode = st.S_IFREG
      size = blob.size
      path = blob.name

    return DirEntry(name=name,
                    path=path,
                    st_mode=mode,
                    st_size=size,
                    st_ctime=blob.time_created.timestamp(),
                    st_mtime=blob.updated.timestamp())

  def listdir(self, path):
    if path:
      if path == '/':
        path = ''
      else:
        path = path + '/' if not path.endswith('/') else path

    dents = dict()

    for blob in self._client.list_blobs(self._bucket, prefix=path):
      if (de := self._blob_stat(blob, base_path=path)) is not None:
        dde = dents.get(de.name)
        if dde is not None:
          de = de._replace(st_ctime=min(de.st_ctime, dde.st_ctime),
                           st_mtime=max(de.st_mtime, dde.st_mtime))

        dents[de.name] = de

    sorted_dents = sorted(dents.items(), key=lambda x: (x[1].st_mode, x[0]))

    for name, de in sorted_dents:
      yield de

  def open(self, path, mode='rb'):
    bucket = self._client.bucket(self._bucket)
    blob = bucket.blob(path)

    return blob.open(mode)

  def upload(self, path, source):
    bucket = self._client.bucket(self._bucket)
    blob = bucket.blob(path)

    with blob.open('wb') as fd:
      for data in source:
        fd.write(data)

  def download(self, path, chunk_size=32 * 1024**2):
    bucket = self._client.bucket(self._bucket)
    blob = bucket.blob(path)

    with blob.open('rb') as fd:
      while True:
        data = fd.read(chunk_size)
        if data:
          yield data
        if chunk_size > len(data):
          break

  def stat(self, path):
    bucket = self._client.bucket(self._bucket)
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
      name = os.path.basename(path[: -1] if path.endswith('/') else path)

      return DirEntry(name=name,
                      st_mode=st.S_IFDIR,
                      st_size=0,
                      st_ctime=ctime,
                      st_mtime=mtime)

  def remove(self, path):
    bucket = self._client.bucket(self._bucket)
    blob = bucket.blob(path)
    blob.delete()

  def rename(self, src_path, dest_path):
    bucket = self._client.bucket(self._bucket)
    src_blob = bucket.blob(src_path)

    bucket.copy_blob(src_blob, bucket, dest_path)
    bucket.delete_blob(src_path)

