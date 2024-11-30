import functools
import os
import struct


_SIZE_FMT = '<Q'
_SIZE_LENGTH = len(struct.pack(_SIZE_FMT, 0))


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



def write_packet(fd, data):
  packet = struct.pack(_SIZE_FMT, len(data)) + data
  ios = IOStream(fd)
  ios.write(packet)


def read_packet(fd):
  ios = IOStream(fd)
  data = ios.read(_SIZE_LENGTH)
  size = struct.unpack(_SIZE_FMT, data)[0]
  packet = ios.read(size)

  return packet

