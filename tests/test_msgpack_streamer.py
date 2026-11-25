"""Tests for py_misc_utils.msgpack_streamer."""

import pytest

from py_misc_utils import fs_utils as fsu

msgpack = pytest.importorskip("msgpack")

from py_misc_utils import msgpack_streamer as ms


def _write(path, records):
  fsu.write_content(path, b''.join(msgpack.packb(recd) for recd in records))


def test_roundtrip(tmp_path):
  path = str(tmp_path / 'data.msgpack')
  recs = [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}, {'a': 3, 'b': 'z'}]
  _write(path, recs)
  out = list(ms.MsgPackStreamer(path))
  assert out == recs


def test_rename_columns(tmp_path):
  path = str(tmp_path / 'data.msgpack')
  recs = [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}]
  _write(path, recs)
  out = list(ms.MsgPackStreamer(path, rename_columns={'a': 'alpha'}))
  assert out == [{'alpha': 1, 'b': 'x'}, {'alpha': 2, 'b': 'y'}]


def test_rename_multiple_columns(tmp_path):
  path = str(tmp_path / 'data.msgpack')
  recs = [{'a': 1, 'b': 2}]
  _write(path, recs)
  out = list(ms.MsgPackStreamer(path, rename_columns={'a': 'x', 'b': 'y'}))
  # Both renames applied; original keys popped.
  assert out == [{'x': 1, 'y': 2}]


def test_empty_file(tmp_path):
  path = str(tmp_path / 'empty.msgpack')
  _write(path, [])
  out = list(ms.MsgPackStreamer(path))
  assert out == []


def test_iter_delegates(tmp_path):
  path = str(tmp_path / 'data.msgpack')
  _write(path, [{'v': 7}])
  streamer = ms.MsgPackStreamer(path)
  assert next(iter(streamer)) == {'v': 7}


def test_missing_file_raises(tmp_path):
  path = str(tmp_path / 'nope.msgpack')
  with pytest.raises(FileNotFoundError):
    list(ms.MsgPackStreamer(path))
