import os
import struct

from . import iostream as ios


_SIZE_PACKER = struct.Struct('<Q')

def write_packet(fd, data):
  packet = _SIZE_PACKER.pack(len(data)) + data
  iofd = ios.IOStream(fd)
  iofd.write(packet)


def read_packet(fd):
  iofd = ios.IOStream(fd)
  data = iofd.read(_SIZE_PACKER.size)
  size = _SIZE_PACKER.unpack(data)[0]
  packet = iofd.read(size)

  return packet

