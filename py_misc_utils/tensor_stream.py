import bisect
import collections
import os
import pickle
import re

import numpy as np
import torch
import torch.utils.data as data_utils

from . import alog
from . import assert_checks as tas
from . import utils as ut


_STATE_FILE = 'state.pkl'


def _check_shapes(prev_shape, new_shape):
  if tuple(prev_shape[1:]) != tuple(new_shape[1:]):
    alog.xraise(RuntimeError, f'Shapes are not compatible: {new_shape} vs {prev_shape}')


def _load_stream_tensors(path):
  stream_tensors = []
  for tname in os.listdir(path):
    # File names within the stream tensors folder is ID.npy.
    tid, ext = os.path.splitext(tname)
    tas.check_eq(ext, '.npy')

    tid = int(tid)
    stream_tensors = ut.idx_expand(stream_tensors, tid)

    tpath = os.path.join(path, tname)
    stream_tensors[tid] = np.lib.format.open_memmap(tpath, mode='r')

  return tuple(stream_tensors)


def _load_tensors(path):
  tensors = []
  for name in os.listdir(path):
    spath = os.path.join(path, name)
    if re.match(r'\d+$', name) and os.path.isdir(spath):
      streamno = int(name)
      tensors = ut.idx_expand(tensors, streamno, filler=())
      tensors[streamno] = _load_stream_tensors(spath)

  return tuple(tensors)


def _get_sizes(tensors):
  sizes = []
  for stream_tensors in tensors:
    stream_sizes = [0]
    for tensor in stream_tensors:
      stream_sizes.append(stream_sizes[-1] + len(tensor))

    sizes.append(tuple(stream_sizes))

  return tuple(sizes)


def _get_shapes(tensors):
  shapes = []
  for stream_tensors in tensors:
    shape = None
    for tensor in stream_tensors:
      if shape is None:
        shape = list(tensor.shape)
      else:
        _check_shapes(shape, tensor.shape)
        shape[0] += len(tensor)

    if shapes and shapes[0][0] != shape[0]:
      alog.xraise(RuntimeError, f'All the tensor streams must have the same major dimension: {shapes[0][0]} vs {shape[0]}')
    shapes.append(tuple(shape))

  return tuple(shapes)


class _ChunkList:

  def __init__(self, init=None):
    self._data = [init] if init is not None else []
    self._size = init.nbytes if init is not None else 0

  def size(self):
    return self._size

  def append(self, t):
    self._data.append(t)
    self._size += t.nbytes

  def coalesce(self):
    return np.concatenate(self._data)


class Writer:

  def __init__(self, path, chunk_size=100 * 1024 * 1024):
    if os.path.exists(path):
      alog.xraise(RuntimeError, f'Tensor stream folder must not exist: {path}')
    os.mkdir(path)
    self._path = path
    self._chunk_size = chunk_size
    self._chunks = []
    self._shapes = []
    self._indices = []

  def write(self, *args):
    size = len(args[0]) if args else 0
    if not self._chunks:
      self._chunks = [_ChunkList(init=t) for t in args]
      self._shapes = [t.shape for t in args]
      self._indices = [0] * len(args)
      for i in range(len(args)):
        if size != len(args[i]):
          alog.xraise(RuntimeError, f'The major dimension of a write operation must match: {size} vs {len(args[i])}')
        os.mkdir(os.path.join(self._path, str(i)))
    else:
      if len(args) != len(self._chunks):
        alog.xraise(RuntimeError, f'Written streams count must match: {len(args)} vs {len(self._chunks)}')
      for i, t in enumerate(args):
        if size != len(t):
          alog.xraise(RuntimeError, f'The major dimension of a write operation must match: {size} vs {len(args[i])}')
        _check_shapes(self._shapes[i], t.shape)
        self._chunks[i].append(t)

    self.flush(final=False)

  def flush(self, final=True, state=None):
    for i, chunk in enumerate(self._chunks):
      if chunk is not None and (final or chunk.size() >= self._chunk_size):
        path = os.path.join(self._path, str(i), str(self._indices[i]) + '.npy')
        np.save(path, chunk.coalesce())

        self._indices[i] += 1
        self._chunks[i] = _ChunkList()

    if state is not None:
      with open(os.path.join(self._path, _STATE_FILE), mode='wb') as f:
        pickle.dump(state, f, protocol=ut.pickle_proto())


class Reader:

  def __init__(self, path, transforms=None):
    if not os.path.isdir(path):
      alog.xraise(RuntimeError, f'Tensor stream folder does not exist: {path}')
    self._path = path
    self._tensors = _load_tensors(path)
    self._sizes = _get_sizes(self._tensors)
    self.shape = _get_shapes(self._tensors)
    self.num_streams = len(self._tensors)
    self.state = dict()
    self._transforms = list(transforms) if transforms else None

    state_path = os.path.join(path, _STATE_FILE)
    if os.path.exists(state_path):
      with open(state_path, mode='rb') as f:
        self.state = pickle.load(f)

  @property
  def dtype(self):
    return tuple([self._tensors[n][0].dtype for n in range(self.num_streams)])

  def __len__(self):
    lens = [self.shape[i][0] for i in range(self.num_streams)]
    tas.check(all(lens[0] == l for l in lens), msg=f'Mismatching sizes: {lens}')

    return lens[0] if lens else 0

  def tensor_sequence(self, streamno):
    if streamno < 0 or streamno >= self.num_streams:
      alog.xraise(RuntimeError, f'Bad stream number {streamno}, must be >= 0 and < {self.num_streams}')

    return self._tensors[streamno]

  def get_slice(self, streamno, start, size=None):
    if streamno < 0 or streamno >= self.num_streams:
      alog.xraise(RuntimeError, f'Bad stream number {streamno}, must be >= 0 and < {self.num_streams}')

    stream_tensors = self._tensors[streamno]
    stream_sizes = self._sizes[streamno]
    stream_shape = self.shape[streamno]

    if start < 0 or start >= stream_shape[0]:
      alog.xraise(IndexError, f'Invalid slice start index {start}, must be >= 0 and < {stream_shape[0]}')

    if size is None:
      size = stream_shape[0] - start

    pos = bisect.bisect_right(stream_sizes, start) - 1
    tensor = stream_tensors[pos]
    tpos = start - stream_sizes[pos]
    tas.check_ge(tpos, 0)

    tsize = min(size, len(tensor) - tpos)
    slices = [tensor[tpos: tpos + tsize]]
    rsize = size - tsize
    while rsize > 0:
      pos += 1
      tensor = stream_tensors[pos]
      tsize = min(rsize, len(tensor))
      rsize -= tsize
      slices.append(tensor[: tsize])

    sliced_tensor = np.concatenate(slices) if len(slices) > 1 else slices[0]
    if self._transforms:
      sliced_tensor = self._transforms[streamno](sliced_tensor)

    return sliced_tensor

  def get_slices(self, start, size=None):
    return [self.get_slice(x, start, size=size) for x in range(self.num_streams)]


class StreamArray(collections.abc.Sequence):

  def __init__(self, reader, streamno):
    super().__init__()
    self.reader = reader
    self.streamno = streamno
    self.shape = reader.shape[streamno]

  def __getitem__(self, i):
    if isinstance(i, slice):
      start, end, step = i.indices(len(self))
      if step != 1:
        parts = []
        for x in range(start, end, step):
          parts.append(self.reader.get_slice(self.streamno, x, size=1))
        return np.concatenate(parts)

      return self.reader.get_slice(self.streamno, start, size=end - start)

    return np.squeeze(self.reader.get_slice(self.streamno, i, size=1), axis=0)

  def __len__(self):
    return self.shape[0]

  def to_numpy(self, dtype=None):
    npa = self.reader.get_slice(self.streamno, 0)

    return npa.astype(dtype) if dtype is not None else npa

  def __array__(self, dtype=None):
    return self.to_numpy(dtype=dtype)


class Dataset(data_utils.Dataset):

  def __init__(self, path, transforms=None):
    super().__init__()
    self.reader = Reader(path, transforms=transforms)

  def __len__(self):
    shape = self.reader.shape
    return shape[0][0] if shape else 0

  def __getitem__(self, i):
    return tuple([torch.from_numpy(x) for x in self.reader.get_slices(i, size=1)])

