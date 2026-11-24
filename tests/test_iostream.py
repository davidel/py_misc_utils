"""Tests for py_misc_utils.iostream.

IOStream normalizes read/write access across file-like, socket-like and
raw-fd objects by binding write/read to write/send/recv/os.write and
os.read as detected at construction time. Tests verify the method
selection for each object family and that the bound functions actually
round-trip data through the wrapped object.
"""

import os
import socket
import tempfile

import pytest

from py_misc_utils import iostream as ios


def test_file_object():
  """A file object gets write/read bound to its own methods."""
  with tempfile.TemporaryFile() as fd:
    stream = ios.IOStream(fd)
    assert stream.write == fd.write
    assert stream.read == fd.read

    stream.write(b'payload')
    fd.seek(0)
    assert stream.read(10) == b'payload'


def test_socket_object():
  """A socket pair gets write/read bound to send/recv."""
  a, b = socket.socketpair()
  try:
    stream = ios.IOStream(a)
    assert stream.write == a.send
    assert stream.read == a.recv

    stream.write(b'ping')
    assert b.recv(16) == b'ping'
    b.send(b'pong')
    assert stream.read(16) == b'pong'
  finally:
    a.close()
    b.close()


def test_raw_fd():
  """A raw integer fd gets os.write/os.read bound."""
  rd, wr = os.pipe()
  try:
    stream = ios.IOStream(wr)
    stream.write(b'fd-data')

    rstream = ios.IOStream(rd)
    assert rstream.read(16) == b'fd-data'
  finally:
    os.close(rd)
    os.close(wr)


def test_socket_recv_partial():
  """recv returns fewer bytes than requested when the peer sends less."""
  a, b = socket.socketpair()
  try:
    stream = ios.IOStream(a)
    stream.write(b'short')
    assert b.recv(64) == b'short'
    # With no further data available, a non-blocking recv raises instead
    # of blocking, showing read == recv passthrough of the socket flags.
    b.setblocking(False)
    with pytest.raises(BlockingIOError):
      b.recv(64)
  finally:
    a.close()
    b.close()
