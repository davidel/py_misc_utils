"""Tests for py_misc_utils.tensor_stream.

Writer/Reader round-trips over temp_path folders, random access via
Reader.get_slice and StreamArray, per-stream transforms, writer state
round-trip, and the empty-stream regression (a stream folder holding no
chunk file must not crash Reader construction).
"""

import os

import numpy as np
import pytest

torch = pytest.importorskip("torch")

from py_misc_utils import tensor_stream as ts


@pytest.fixture()
def stream_path(temp_path):
  return os.path.join(temp_path, 'stream')


def _mk_writer(path, chunk_size=64):
  return ts.Writer(path, chunk_size=chunk_size)


def test_writer_folder_must_not_exist(stream_path):
  os.makedirs(stream_path)
  with pytest.raises(RuntimeError):
    ts.Writer(stream_path, chunk_size=64)


def test_write_creates_subfolders(stream_path):
  w = _mk_writer(stream_path)
  a = np.zeros((2, 3), dtype=np.int32)
  b = np.zeros((2, 2), dtype=np.float32)
  w.write(a, b)
  w.flush(final=True)
  assert sorted(os.listdir(stream_path)) == ['0', '1']
  assert os.listdir(os.path.join(stream_path, '0')) == ['0.npy']


def test_first_write_fixes_stream_count(stream_path):
  w = _mk_writer(stream_path)
  w.write(np.zeros((2, 3), dtype=np.int32))
  with pytest.raises(RuntimeError):
    w.write(np.zeros((2, 3), dtype=np.int32), np.zeros((2, 3), dtype=np.int32))


def test_major_dim_mismatch_raises(stream_path):
  # Tensors of a single write() must all have the same major dimension.
  w = _mk_writer(stream_path)
  with pytest.raises(RuntimeError):
    w.write(np.zeros((4, 3), dtype=np.int32), np.zeros((2, 2), dtype=np.float32))


def test_shape_mismatch_raises(stream_path):
  w = _mk_writer(stream_path)
  w.write(np.zeros((2, 3), dtype=np.int32))
  with pytest.raises(RuntimeError):
    w.write(np.zeros((2, 4), dtype=np.int32))


def test_chunk_flushing(stream_path):
  # chunk_size=64 bytes: an int32 (x, 2) sample is 8 bytes => 8 samples/chunk.
  # Chunks are flushed at the END of each write() call, so each write of
  # 8+ samples produces one chunk file.
  w = _mk_writer(stream_path, chunk_size=64)
  a = np.arange(24, dtype=np.int32).reshape(12, 2)
  w.write(a[:8])
  w.write(a[8:])
  w.flush(final=True)
  chunks = sorted(os.listdir(os.path.join(stream_path, '0')))
  assert chunks == ['0.npy', '1.npy']
  r = ts.Reader(stream_path)
  got = r.get_slice(0, 0)
  assert got.dtype == np.int32
  assert np.array_equal(got, a)
  # Reading across the chunk boundary works transparently.
  assert np.array_equal(r.get_slice(0, 6, size=4), a[6:10])


def test_roundtrip_multi_stream(stream_path):
  w = _mk_writer(stream_path)
  a = np.arange(10, dtype=np.int32).reshape(5, 2)
  b = np.arange(100, 105, dtype=np.float32).reshape(5, 1)
  w.write(a, b)
  w.flush(final=True, state={'epoch': 3})
  r = ts.Reader(stream_path)
  assert r.num_streams == 2
  assert r.shape == ((5, 2), (5, 1))
  assert r.dtype == (np.dtype('int32'), np.dtype('float32'))
  assert len(r) == 5
  assert r.state == {'epoch': 3}
  assert np.array_equal(r.get_slice(0, 0), a)
  assert np.array_equal(r.get_slice(1, 0), b)


def test_state_roundtrip(stream_path):
  w = _mk_writer(stream_path)
  w.write(np.zeros((3, 2), dtype=np.int32))
  w.flush(final=True, state={'pos': 42, 'name': 'x'})
  r = ts.Reader(stream_path)
  assert r.state == {'pos': 42, 'name': 'x'}
  assert ts.Reader.is_stream_folder(stream_path) is True


def test_reader_missing_folder_raises(temp_path):
  with pytest.raises(RuntimeError):
    ts.Reader(os.path.join(temp_path, 'missing'))


def test_get_slice_partial(stream_path):
  w = _mk_writer(stream_path)
  a = np.arange(20, dtype=np.int32).reshape(10, 2)
  w.write(a)
  w.flush(final=True)
  r = ts.Reader(stream_path)
  got = r.get_slice(0, 2, size=3)
  assert np.array_equal(got, a[2:5])
  got_tail = r.get_slice(0, 8)
  assert np.array_equal(got_tail, a[8:])


def test_transforms(stream_path):
  w = _mk_writer(stream_path)
  a = np.arange(6, dtype=np.int32).reshape(3, 2)
  b = np.arange(3, dtype=np.float32).reshape(3, 1)
  w.write(a, b)
  w.flush(final=True)
  r = ts.Reader(stream_path, transforms=[lambda x: x * 2, lambda x: x + 100])
  got0 = r.get_slice(0, 0)
  got1 = r.get_slice(1, 0)
  assert np.array_equal(got0, a * 2)
  assert np.array_equal(got1, b + 100)


def test_empty_stream_reader_regression(stream_path):
  # Regression: a stream folder with no chunk file (zero samples written)
  # used to crash Reader construction with TypeError: 'NoneType' object is
  # not iterable in _get_shapes. The fallback shape (0,) keeps Reader
  # construction working.
  w = _mk_writer(stream_path)
  w.write(np.zeros((0, 2), dtype=np.int32))
  w.flush(final=True)
  r = ts.Reader(stream_path)
  assert r.num_streams == 1
  assert r.shape == ((0,),)
  assert len(r) == 0
  # Out-of-range start is still an IndexError - as for non-empty streams.
  with pytest.raises(IndexError):
    r.get_slice(0, 0)


def test_stream_array_indexing(stream_path):
  w = _mk_writer(stream_path)
  a = np.arange(10, dtype=np.int32).reshape(5, 2)
  b = np.arange(100, 110, dtype=np.float32).reshape(5, 2)
  w.write(a, b)
  w.flush(final=True)
  r = ts.Reader(stream_path)
  sa = ts.StreamArray(r, 0)
  assert len(sa) == 5
  assert np.array_equal(sa[2], a[2])
  assert sa[2].shape == (2,)
  assert np.array_equal(sa[1:4], a[1:4])
  # Step != 1 goes through the concatenate path.
  assert np.array_equal(sa[::2], a[::2])
  assert np.array_equal(sa.to_numpy(), a)
  assert np.array_equal(sa.to_numpy(dtype=np.int64), a.astype(np.int64))
  assert np.array_equal(np.asarray(sa), a)


def test_stream_array_stream1(stream_path):
  w = _mk_writer(stream_path)
  a = np.arange(10, dtype=np.int32).reshape(5, 2)
  b = np.arange(100, 110, dtype=np.float32).reshape(5, 2)
  w.write(a, b)
  w.flush(final=True)
  r = ts.Reader(stream_path)
  sb = ts.StreamArray(r, 1)
  assert np.array_equal(np.asarray(sb), b)
  assert sb[0].dtype == np.float32


def test_dataset(stream_path):
  w = _mk_writer(stream_path)
  a = np.arange(10, dtype=np.int32).reshape(5, 2)
  b = np.arange(100, 110, dtype=np.float32).reshape(5, 2)
  w.write(a, b)
  w.flush(final=True)
  ds = ts.Dataset(stream_path)
  assert len(ds) == 5
  x0, x1 = ds[0]
  assert isinstance(x0, torch.Tensor) and isinstance(x1, torch.Tensor)
  # get_slices(i, size=1) returns shape (1, 2) tensors - no squeeze.
  assert tuple(x0.shape) == (1, 2)
  assert torch.equal(x0, torch.from_numpy(a[0:1]))
  assert torch.equal(x1, torch.from_numpy(b[0:1]))
  x3, _ = ds[3]
  assert torch.equal(x3, torch.from_numpy(a[3:4]))
