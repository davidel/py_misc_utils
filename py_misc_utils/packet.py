"""Packet framing utilities for byte streams.

This module provides functions to write and read length-prefixed packets
over file descriptors using IOStream. Each packet is framed with an 8-byte
little-endian unsigned length prefix followed by the raw payload data.
"""

import os
import struct

from . import iostream as ios

_SIZE_PACKER = struct.Struct('<Q')


def write_packet(fd, data):
  """Write a length-prefixed packet to a file descriptor.

  The packet consists of an 8-byte little-endian unsigned length prefix
  followed by the raw data bytes.

  Args:
    fd: The file descriptor or stream to write to.
    data: The payload bytes to write.
  """
  packet = _SIZE_PACKER.pack(len(data)) + data
  iofd = ios.IOStream(fd)
  iofd.write(packet)


def read_packet(fd):
  """Read a length-prefixed packet from a file descriptor.

  Reads an 8-byte little-endian unsigned length prefix, then reads that many
  bytes of payload.

  Args:
    fd: The file descriptor or stream to read from.

  Returns:
    The payload bytes of the packet.
  """
  iofd = ios.IOStream(fd)
  data = iofd.read(_SIZE_PACKER.size)
  size = _SIZE_PACKER.unpack(data)[0]
  packet = iofd.read(size)

  return packet
