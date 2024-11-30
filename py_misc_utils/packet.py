import os
import struct


_SIZE_FMT = '<Q'
_SIZE_LENGTH = len(struct.pack(_SIZE_FMT, 0))


def write_packet(fd, data):
  packet = struct.pack(_SIZE_FMT, len(data)) + data
  if hasattr(fd, 'write'):
    fd.write(packet)
  elif hasattr(fd, 'send'):
    fd.send(packet)
  else:
    os.write(fd, packet)


def read_packet(fd):
  if hasattr(fd, 'read'):
    data = fd.read(_SIZE_LENGTH)
    size = struct.unpack(_SIZE_FMT, data)[0]
    packet = fd.read(size)
  elif hasattr(fd, 'recv'):
    data = fd.recv(_SIZE_LENGTH)
    size = struct.unpack(_SIZE_FMT, data)[0]
    packet = fd.recv(size)
  else:
    data = os.read(fd, _SIZE_LENGTH)
    size = struct.unpack(_SIZE_FMT, data)[0]
    packet = os.read(fd, size)

  return packet

