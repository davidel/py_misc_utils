import os
import struct


def write_packet(fd, data):
  packet = struct.pack('<I', len(data)) + data
  if hasattr(fd, 'write'):
    fd.write(packet)
  else:
    os.write(fd, packet)


def read_packet(fd):
  if hasattr(fd, 'read'):
    data = fd.read(4)
    size = struct.unpack('<I', data)[0]
    packet = fd.read(size)
  else:
    data = os.read(fd, 4)
    size = struct.unpack('<I', data)[0]
    packet = os.read(fd, size)

  return packet

