import collections


DirEntry = collections.namedtuple(
  'DirEntry',
  'name, st_mode, st_size, st_ctime, st_mtime, path, etag',
  defaults=(None, None)
)

