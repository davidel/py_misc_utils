import collections


DirEntry = collections.namedtuple(
  'DirEntry',
  'name, st_mode, st_size, st_ctime, st_mtime, path, etag',
  defaults=(None,) * 7
)


def extended_entry(name, fields):
  de_fields = list(DirEntry._fields)
  for field in fields:
    if field not in de_fields:
      de_fields.append(field)

  return collections.namedtuple(name, tuple(de_fields), defaults=(None,) * len(de_fields))

