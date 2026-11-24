"""Tests for py_misc_utils.packet.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): write_packet() frames data with an 8-byte little-endian
unsigned length prefix; read_packet() reads the prefix then exactly that
many payload bytes; the two compose over any file-like object and over
raw OS file descriptors (via IOStream).
"""

import io
import os
import tempfile

from py_misc_utils import packet as pkt


def test_roundtrip_bytesio():
  """Packets survive a write/read roundtrip over BytesIO.

  Note: write_packet leaves the stream position at the end, so reads
  require an explicit seek(0) first (observed contract).
  """
  buf = io.BytesIO()
  pkt.write_packet(buf, b'hello')
  pkt.write_packet(buf, b'world')
  buf.seek(0)

  assert pkt.read_packet(buf) == b'hello'
  assert pkt.read_packet(buf) == b'world'


def test_frame_layout():
  """The frame is an 8-byte little-endian length prefix + payload."""
  buf = io.BytesIO()
  pkt.write_packet(buf, b'AB')

  raw = buf.getvalue()
  assert len(raw) == 10
  assert raw[:8] == (2).to_bytes(8, 'little')
  assert raw[8:] == b'AB'


def test_empty_payload():
  """Empty payloads roundtrip as zero-length frames."""
  buf = io.BytesIO()
  pkt.write_packet(buf, b'')
  buf.seek(0)

  assert pkt.read_packet(buf) == b''
  assert len(buf.getvalue()) == 8


def test_binary_payload_with_newlines():
  """Payloads containing newlines and NULs are preserved verbatim."""
  data = b'a\nb\x00c\r\n'
  buf = io.BytesIO()
  pkt.write_packet(buf, data)
  buf.seek(0)

  assert pkt.read_packet(buf) == data


def test_roundtrip_real_fd():
  """Packets roundtrip through a real file descriptor."""
  with tempfile.TemporaryFile() as fpt:
    pkt.write_packet(fpt, b'payload')
    fpt.seek(0)

    assert pkt.read_packet(fpt) == b'payload'


def test_large_payload():
  """A 64 KiB payload survives the roundtrip."""
  data = os.urandom(65536)
  buf = io.BytesIO()
  pkt.write_packet(buf, data)
  buf.seek(0)

  assert pkt.read_packet(buf) == data
