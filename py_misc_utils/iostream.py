import functools
import os


class IOStream:

  def __init__(self, fd):
    if (wfn := getattr(fd, 'write', None)) is not None:
      self.write = wfn
    elif (wfn := getattr(fd, 'send', None)) is not None:
      self.write = wfn
    else:
      self.write = functools.partial(os.write, fd)

    if (rfn := getattr(fd, 'read', None)) is not None:
      self.read = rfn
    elif (rfn := getattr(fd, 'recv', None)) is not None:
      self.read = rfn
    else:
      self.read = functools.partial(os.read, fd)

