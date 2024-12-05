import abc
import collections
import stat as st


DirEntry = collections.namedtuple(
  'DirEntry',
  'name, st_mode, st_size, st_ctime, st_mtime, path, etag',
  defaults=(None, None)
)


class FsBase(abc.ABC):

  def __init__(self, cache_iface=None, **kwargs):
    super().__init__()
    self._cache_iface = cache_iface
    self._kwargs = kwargs

  @classmethod
  def read_mode(cls, mode):
    return 'r' in mode and not '+' in mode

  @classmethod
  def write_mode(cls, mode):
    return any(c in mode for c in 'wa+')

  @classmethod
  def truncate_mode(cls, mode):
    return 'w' in mode and '+' not in mode

  @classmethod
  def append_mode(cls, mode):
    return 'a' in mode

  @classmethod
  def text_mode(cls, mode):
    return 'b' not in mode

  def norm_url(self, url):
    return url

  def exists(self, url):
    try:
      self.stat(url)

      return True
    except:
      return False

  def isdir(self, url):
    try:
      de = self.stat(url)

      return st.S_ISDIR(de.st_mode)
    except:
      return False

  def isfile(self, url):
    try:
      de = self.stat(url)

      return st.S_ISREG(de.st_mode)
    except:
      return False

  def copyfile(self, url, dest_fs, dest_url):
    dest_fs.put_file(dest_url, self.get_file(url))

  @abc.abstractmethod
  def stat(self, url):
    ...

  @abc.abstractmethod
  def open(self, url, mode, **kwargs):
    ...

  @abc.abstractmethod
  def remove(self, url):
    ...

  @abc.abstractmethod
  def rename(self, src_url, dest_url):
    ...

  def replace(self, src_url, dest_url):
    self.rename(src_url, dest_url)

  @abc.abstractmethod
  def mkdir(self, url, mode=None):
    ...

  @abc.abstractmethod
  def makedirs(self, url, mode=None, exist_ok=None):
    ...

  @abc.abstractmethod
  def rmdir(self, url):
    ...

  @abc.abstractmethod
  def rmtree(self, url, ignore_errors=None):
    ...

  @abc.abstractmethod
  def list(self, url):
    ...

  @abc.abstractmethod
  def put_file(self, url, data_gen):
    ...

  @abc.abstractmethod
  def get_file(self, url):
    ...

