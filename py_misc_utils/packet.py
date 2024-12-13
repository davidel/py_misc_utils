import os
import struct

from . import iostream as ios


_SIZE_FMT = '<Q'
_SIZE_LENGTH = len(struct.pack(_SIZE_FMT, 0))


def write_packet(fd, data):
  packet = struct.pack(_SIZE_FMT, len(data)) + data
  iofd = ios.IOStream(fd)
  iofd.write(packet)


def read_packet(fd):
  iofd = ios.IOStream(fd)
  data = iofd.read(_SIZE_LENGTH)
  size = struct.unpack(_SIZE_FMT, data)[0]
  packet = iofd.read(size)

  return packet

