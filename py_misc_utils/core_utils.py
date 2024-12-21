# This module is for APIs which has no local dependecies.

def size_str(size):
  syms = ('B', 'KB', 'MB', 'GB', 'TB')

  for i, sym in enumerate(syms):
    if size < 1024:
      return f'{size} {sym}' if i == 0 else f'{size:.2f} {sym}'

    size /= 1024

  return f'{size * 1024:.2f} {syms[-1]}'

