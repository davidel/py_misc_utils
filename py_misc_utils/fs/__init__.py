"""FileSystem backends for remote and local storage schemes.

This package groups the FileSystem implementations selected through the
FILE_SYSTEMS tuple of every submodule, so that the generic open/copy/list
helpers can dispatch on the scheme of a path or URL. The
py_misc_utils.fs_base.FsBase base class defines the contract shared by all
of them; this module documents the contract from the perspective of the
backends, while py_misc_utils.gfs documents the top-level API dispatching
over them.

The backend contract:
- A backend is a FsBase subclass exposing an ID attribute - its primary
  scheme name - and an IDS tuple listing every scheme it serves (e.g.
  ('http', 'https')).
- Backends receive cache settings at construction time: the
  cache_iface keyword holds the py_misc_utils.cached_file.CacheInterface
  used to serve remote reads through the block cache - typically built
  by gfs.resolve_fs() from the configured cache_dir, but a custom one can
  be passed to control or disable caching.
- Reads are served through the block cache: backends implement the
  reader fetching the file size and the blocks - or the whole content for
  backends not supporting ranged reads - and delegate the cached,
  seekable file object to CacheInterface.open().
- Writes are staged into a temporary file - the existing content is
  downloaded first when the mode does not truncate - and uploaded when
  the stream is closed (writeback semantics).
- Text modes wrap the returned stream into an io.TextIOWrapper.

Which methods a backend implements:
- Required by FsBase: stat, open, remove, rename, mkdir, makedirs, rmdir,
  rmtree, list, put_file, get_file, as_local, link and symlink.
- Provided or adapted by FsBase: exists, isdir and isfile (built on top
  of stat), replace (delegating to rename), copyfile (streaming
  get_file/put_file between backends), the mode classification helpers
  (read_mode, write_mode, truncate_mode, append_mode and text_mode) and
  norm_url (URL/path normalization, returning the full URL for remote
  backends).

The schemes registered by the built-in backends:
- file: local files (fs.file_fs.FileFs)
- ftp: FTP servers (fs.ftp_fs.FtpFs)
- http and https: web servers exposing directory listings
  (fs.http_fs.HttpFs)
- gs: Google Cloud Storage (fs.gcs_fs.GcsFs)
- s3: Amazon S3 (fs.s3_fs.S3Fs)

Backends are registered lazily by gfs.register_modules(), which imports
every *_fs module of this package and of the colon-separated GFS_PATH
environment variable, and looks up the FILE_SYSTEMS tuple of each of
them. Backends can also be registered programmatically through
gfs.register_fs().

Example:
  import io
  import os
  import stat
  import tempfile

  from py_misc_utils import fs_base as fsb
  from py_misc_utils import gfs

  # A minimal custom backend: an in-memory store served through the
  # block-cache machinery, like any remote backend.
  class MemFs(fsb.FsBase):
    ID = 'mem'
    IDS = (ID,)

    def __init__(self, cache_iface=None, **kwargs):
      super().__init__(cache_iface=cache_iface, **kwargs)
      self.store = dict()

    def stat(self, url):
      data = self.store.get(url)
      if data is None:
        return None
      return fsb.DirEntry(name=os.path.basename(url),
                          st_mode=stat.S_IFREG,
                          st_size=len(data),
                          st_ctime=0,
                          st_mtime=0)

    def open(self, url, mode, **kwargs):
      return io.BytesIO(self.store.get(url, b''))

    def remove(self, url):
      self.store.pop(url, None)

    def rename(self, src_url, dest_url):
      self.store[dest_url] = self.store.pop(src_url, b'')

    def mkdir(self, url, mode=None):
      pass

    def makedirs(self, url, mode=None, exist_ok=None):
      pass

    def rmdir(self, url):
      pass

    def rmtree(self, url, ignore_errors=None):
      pass

    def list(self, url):
      return iter(())

    def put_file(self, url, data_gen):
      self.store[url] = b''.join(data_gen)

    def get_file(self, url):
      return iter((self.store.get(url, b''),))

    def as_local(self, url):
      return None

    def link(self, src_url, dest_url):
      self.store[dest_url] = self.store.get(src_url, b'')

    def symlink(self, src_url, dest_url):
      self.link(src_url, dest_url)

  # Register it, then build it through the standard dispatch machinery -
  # resolve_fs() wires the cache interface for us.
  gfs.register_fs(MemFs)

  root = tempfile.mkdtemp()
  fs, fpath = gfs.resolve_fs('mem://host/data.txt', cache_dir=root)
  fs.put_file('mem://host/data.txt', [b'hello ', b'mem'])
  print(b''.join(fs.get_file('mem://host/data.txt')))

  # The built-in backends are registered the same way - lazily - by
  # register_modules(), which also imports custom *_fs.py modules found
  # in the directories listed by the GFS_PATH environment variable.
  os.environ['GFS_PATH'] = '/path/to/my/backends'
  gfs.register_modules()
  file_fs = gfs.get_proto_fs('file', cache_dir=root)
  print(file_fs.ID)
"""
