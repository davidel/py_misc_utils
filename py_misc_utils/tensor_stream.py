"""Tensor stream support.

This module implements a simple on-disk tensor storage format (a "tensor
stream") which allows to append - possibly repeatedly - tensors to a data
store, and then to read back the stored data in a random access fashion.

A tensor stream is stored inside a folder which contains one sub-folder per
stream, each of them holding a sequence of ``.npy`` files (chunks) which are
memory-mapped when accessed. Optionally, a state file can be stored in the
tensor stream folder as well, so that arbitrary metadata - like the epoch at
which the stream has been written - can be persisted along with the data.

Writing a tensor stream::

  writer = Writer('/data/mystream')
  writer.write(feats, labels)  # Writes one batch of samples.
  writer.write(feats, labels)  # Writes another batch of samples.
  writer.flush()               # Flushes pending data to disk.

Reading a tensor stream::

  reader = Reader('/data/mystream')
  feats = reader.get_slice(0, start=10, size=5)

Tensor streams can also be accessed as if they were numpy arrays (see
StreamArray) or PyTorch datasets (see Dataset).

"""
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
from . import core_utils as cu
from . import utils as ut

_STATE_FILE = 'tensor_stream_state.pkl'


def _check_shapes(prev_shape, new_shape):
  """Checks that two tensor shapes are compatible.

  Only the trailing dimensions (all but the major dimension) are compared.

  Args:
    prev_shape: The previously seen tensor shape.
    new_shape: The newly seen tensor shape.

  Raises:
    RuntimeError: If the trailing dimensions of the shapes differ.
  """
  if tuple(prev_shape[1:]) != tuple(new_shape[1:]):
    alog.xraise(RuntimeError, f'Shapes are not compatible: {new_shape} vs {prev_shape}')


def _load_stream_tensors(path):
  """Loads all the tensors stored in a single-stream tensor stream folder.

  Args:
    path: The folder containing the ``.npy`` files of a single stream.

  Returns:
    A tuple of memory-mapped tensors, indexed by their ID found in the
    file name.
  """
  stream_tensors = []
  for tname in os.listdir(path):
    # File names within the stream tensors folder is ID.npy.
    tid, ext = os.path.splitext(tname)
    tas.check_eq(ext, '.npy')

    tid = int(tid)
    stream_tensors = cu.idx_expand(stream_tensors, tid)

    tpath = os.path.join(path, tname)
    stream_tensors[tid] = np.lib.format.open_memmap(tpath, mode='r')

  return tuple(stream_tensors)


def _load_tensors(path):
  """Loads all the streams stored in a tensor stream folder.

  Args:
    path: The tensor stream folder, containing one sub-folder per stream.

  Returns:
    A tuple of tuples of memory-mapped tensors, one entry per stream.
  """
  tensors = []
  for name in os.listdir(path):
    spath = os.path.join(path, name)
    if re.match(r'\d+$', name) and os.path.isdir(spath):
      streamno = int(name)
      tensors = cu.idx_expand(tensors, streamno, filler=())
      tensors[streamno] = _load_stream_tensors(spath)

  return tuple(tensors)


def _get_sizes(tensors):
  """Computes the cumulative sizes of the tensors of each stream.

  Args:
    tensors: A tuple of tuples of tensors, one entry per stream.

  Returns:
    A tuple - one entry per stream - of tuples of cumulative sizes. Each
    tuple contains ``len(tensor) + 1`` entries, starting with 0, so that
    the chunk holding a given element index can be quickly found by binary
    search.
  """
  sizes = []
  for stream_tensors in tensors:
    stream_sizes = [0]
    for tensor in stream_tensors:
      stream_sizes.append(stream_sizes[-1] + len(tensor))

    sizes.append(tuple(stream_sizes))

  return tuple(sizes)


def _get_shapes(tensors):
  """Computes the overall shapes of each stream.

  Args:
    tensors: A tuple of tuples of tensors, one entry per stream.

  Returns:
    A tuple of shapes - one per stream.

  Raises:
    RuntimeError: If tensors of a stream have mismatching trailing
      dimensions, or if the streams have different major dimensions.
  """
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
      alog.xraise(
          RuntimeError,
          f'All the tensor streams must have the same major dimension: {shapes[0][0]} vs {shape[0]}'
      )
    shapes.append(tuple(shape))

  return tuple(shapes)


class _ChunkList:
  """A list of tensors holding the chunk of data pending flush to disk."""

  def __init__(self, init=None):
    """Creates a chunk list.

    Args:
      init: An optional tensor to initialize the chunk list with.
    """
    self._data = [init] if init is not None else []
    self._size = init.nbytes if init is not None else 0

  def size(self):
    """Returns the total number of bytes held by the chunk list."""

    return self._size

  def append(self, t):
    """Appends a tensor to the chunk list.

    Args:
      t: The tensor to append. The number of major-dimension elements must
        match the tensors already held by the chunk list.
    """
    self._data.append(t)
    self._size += t.nbytes

  def coalesce(self):
    """Concatenates the tensors of the chunk list into a single tensor."""

    return np.concatenate(self._data)


class Writer:
  """A tensor stream writer.

  A writer accumulates tensors - one per stream - in memory, and flushes
  them to the tensor stream folder as chunk files whenever the amount of
  pending data reaches a given chunk size.
  """

  def __init__(self, path, chunk_size=100 * 1024 * 1024):
    """Creates a tensor stream writer.

    Args:
      path: The tensor stream folder path. It must not already exist.
      chunk_size: The amount of data (in bytes) accumulated before a chunk
        is written to disk.

    Raises:
      RuntimeError: If the tensor stream folder already exists.
    """
    if os.path.exists(path):
      alog.xraise(RuntimeError, f'Tensor stream folder must not exist: {path}')
    os.mkdir(path)
    self._path = path
    self._chunk_size = chunk_size
    self._chunks = []
    self._shapes = []
    self._indices = []

  # Note that the tensors handed over to the write() API will become owned by
  # the Writer obect, and cannot be written over after the write operation.
  def write(self, *args):
    """Write a batch of tensors to the stream.

    The tensors handed over become owned by the Writer object, and cannot
    be written over after the write operation.

    Args:
      *args: The tensors to write - one per configured stream, aligned in
        the major dimension.

    Raises:
      RuntimeError: When the tensor counts or major dimensions mismatch
        the configured streams.
    """
    size = len(args[0]) if args else 0
    if not self._chunks:
      self._chunks = [_ChunkList(init=t) for t in args]
      self._shapes = [t.shape for t in args]
      self._indices = [0] * len(args)
      for i in range(len(args)):
        if size != len(args[i]):
          alog.xraise(
              RuntimeError,
              f'The major dimension of a write operation must match: {size} vs {len(args[i])}'
          )
        os.mkdir(os.path.join(self._path, str(i)))
    else:
      if len(args) != len(self._chunks):
        alog.xraise(
            RuntimeError,
            f'Written streams count must match: {len(args)} vs {len(self._chunks)}')
      for i, t in enumerate(args):
        if size != len(t):
          alog.xraise(
              RuntimeError,
              f'The major dimension of a write operation must match: {size} vs {len(args[i])}'
          )
        _check_shapes(self._shapes[i], t.shape)
        self._chunks[i].append(t)

    self.flush(final=False)

  def flush(self, final=True, state=None):
    """Flushes pending chunks of data to disk.

    Args:
      final: If True, all the non-empty pending chunks are written to disk.
        Otherwise only the chunks whose size reached the chunk size are
        written.
      state: An optional object to persist into the tensor stream state
        file - useful to store metadata (like epoch numbers) along the
        tensor stream data.
    """
    for i, chunk in enumerate(self._chunks):
      if chunk is not None and chunk.size() > 0 and (final or
                                                     chunk.size() >= self._chunk_size):
        path = os.path.join(self._path, str(i), str(self._indices[i]) + '.npy')
        np.save(path, chunk.coalesce())

        self._indices[i] += 1
        self._chunks[i] = _ChunkList()

    if state is not None:
      with open(os.path.join(self._path, _STATE_FILE), mode='wb') as f:
        pickle.dump(state, f, protocol=ut.pickle_proto())


class Reader:
  """A tensor stream reader.

  A reader gives random access to the tensors stored in a tensor stream
  folder, optionally applying per-stream transformation functions to the
  data being read.

  Attributes:
    shape: A tuple of shapes - one per stream.
    num_streams: The number of streams stored in the tensor stream folder.
    state: The state object persisted by the writer, if any.
  """

  def __init__(self, path, transforms=None):
    """Creates a tensor stream reader.

    Args:
      path: The tensor stream folder path.
      transforms: An optional list - one entry per stream - of functions
        taking a tensor and returning a transformed tensor, applied to the
        data before it is returned by the read operations.

    Raises:
      RuntimeError: If the tensor stream folder does not exist.
    """
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

  @classmethod
  def is_stream_folder(cls, path):
    """Checks whether a folder holds a tensor stream with a state file.

    Args:
      path: The folder path to check.

    Returns:
      True if the folder contains a tensor stream state file.
    """
    state_path = os.path.join(path, _STATE_FILE)

    return os.path.exists(state_path)

  @property
  def dtype(self):
    """The dtype of each stream."""

    return tuple([self._tensors[n][0].dtype for n in range(self.num_streams)])

  def __len__(self):
    """Returns the number of samples stored in each stream."""

    lens = [self.shape[i][0] for i in range(self.num_streams)]
    tas.check(all(lens[0] == l for l in lens), msg=f'Mismatching sizes: {lens}')

    return lens[0] if lens else 0

  def tensor_sequence(self, streamno):
    """Returns the sequence of memory-mapped tensors of a stream.

    Args:
      streamno: The stream number.

    Returns:
      A tuple of memory-mapped tensors - one per chunk file of the stream.

    Raises:
      RuntimeError: If the stream number is out of range.
    """
    if streamno < 0 or streamno >= self.num_streams:
      alog.xraise(
          RuntimeError,
          f'Bad stream number {streamno}, must be >= 0 and < {self.num_streams}')

    return self._tensors[streamno]

  def get_slice(self, streamno, start, size=None):
    """Reads a slice of a stream.

    The slice may span multiple chunk files, in which case the chunks are
    transparently concatenated.

    Args:
      streamno: The stream number.
      start: The start index of the slice within the stream.
      size: The number of samples to read. If None, all the samples from
        start to the end of the stream are read.

    Returns:
      The sliced tensor, with its major dimension holding at most size
      elements, after having applied the stream transformation function -
      if any.

    Raises:
      RuntimeError: If the stream number is out of range.
      IndexError: If the slice start index is out of range.
    """
    if streamno < 0 or streamno >= self.num_streams:
      alog.xraise(
          RuntimeError,
          f'Bad stream number {streamno}, must be >= 0 and < {self.num_streams}')

    stream_tensors = self._tensors[streamno]
    stream_sizes = self._sizes[streamno]
    stream_shape = self.shape[streamno]

    if start < 0 or start >= stream_shape[0]:
      alog.xraise(
          IndexError,
          f'Invalid slice start index {start}, must be >= 0 and < {stream_shape[0]}')

    if size is None:
      size = stream_shape[0] - start
    else:
      size = min(size, stream_shape[0] - start)

    pos = bisect.bisect_right(stream_sizes, start) - 1
    tensor = stream_tensors[pos]
    tpos = start - stream_sizes[pos]
    tas.check_ge(tpos, 0)

    tsize = min(size, len(tensor) - tpos)
    slices = [tensor[tpos:tpos + tsize]]
    rsize = size - tsize
    while rsize > 0:
      pos += 1
      tensor = stream_tensors[pos]
      tsize = min(rsize, len(tensor))
      rsize -= tsize
      slices.append(tensor[:tsize])

    sliced_tensor = np.concatenate(slices) if len(slices) > 1 else slices[0]
    if self._transforms:
      sliced_tensor = self._transforms[streamno](sliced_tensor)

    return sliced_tensor

  def get_slices(self, start, size=None):
    """Reads a slice of each stream.

    Args:
      start: The start index of the slices within the streams.
      size: The number of samples to read from each stream. If None, all
        the samples from start to the end of the streams are read.

    Returns:
      A list of sliced tensors - one per stream.
    """
    return [self.get_slice(x, start, size=size) for x in range(self.num_streams)]


class StreamArray(collections.abc.Sequence):
  """A stream seen as a 1-D sequence of numpy array samples.

  StreamArray objects support indexing (both integer and slice indexing)
  and behave as if they were numpy arrays (via the __array__ protocol).
  """

  def __init__(self, reader, streamno):
    """Creates a stream array.

    Args:
      reader: The Reader object holding the stream.
      streamno: The stream number.
    """
    super().__init__()
    self.reader = reader
    self.streamno = streamno
    self.shape = reader.shape[streamno]

  def __getitem__(self, i):
    """Reads one sample - or a slice of samples - of the stream.

    Args:
      i: An integer index, or a slice object.

    Returns:
      A numpy array holding the sample (dims: the stream shape without the
      major dimension) if i is an integer index, or a numpy array holding
      the requested samples if i is a slice.
    """
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
    """Returns the number of samples of the stream."""

    return self.shape[0]

  def to_numpy(self, dtype=None):
    """Reads the whole stream as a numpy array.

    Args:
      dtype: An optional numpy dtype the stream is casted to.

    Returns:
      The whole stream data as a numpy array.
    """
    npa = self.reader.get_slice(self.streamno, 0)

    return npa.astype(dtype) if dtype is not None else npa

  def __array__(self, dtype=None):
    """Returns the whole stream as a numpy array (numpy protocol)."""

    return self.to_numpy(dtype=dtype)


class Dataset(data_utils.Dataset):
  """A tensor stream seen as a PyTorch dataset.

  Each dataset item is a tuple of tensors - one per stream - holding the
  data of a single sample.
  """

  def __init__(self, path, transforms=None):
    """Creates a PyTorch dataset backed by a tensor stream.

    Args:
      path: The tensor stream folder path.
      transforms: An optional list - one entry per stream - of functions
        taking a tensor and returning a transformed tensor, applied to the
        data before it is returned by the read operations.
    """
    super().__init__()
    self.reader = Reader(path, transforms=transforms)

  def __len__(self):
    """Returns the number of samples held by the tensor stream."""

    shape = self.reader.shape
    return shape[0][0] if shape else 0

  def __getitem__(self, i):
    """Reads one sample of the tensor stream.

    Args:
      i: The index of the sample to read.

    Returns:
      A tuple of torch tensors - one per stream - holding the sample data.
    """
    return tuple([torch.from_numpy(x) for x in self.reader.get_slices(i, size=1)])
