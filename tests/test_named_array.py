"""Tests for py_misc_utils.named_array.

Expectations follow the documented API contract. Tests assert specific
values verified by probe scripts; where library behavior deviates from
the documented contract the test will fail, exposing a possible library
bug.
"""

import numpy as np
import pandas as pd
import pytest

from py_misc_utils import named_array as na


def test_field_append_and_np_array():
  """Field stores values via NamedArray; np_array() returns them as numpy."""
  arr = na.NamedArray('val=i')
  arr.kwappend(val=10)
  arr.kwappend(val=20)
  arr.kwappend(val=30)

  result = arr.get_array('val')
  assert result.tolist() == [10, 20, 30]
  assert result.dtype == np.int32


def test_field_append_string():
  """NamedArray supports string fields ('S' format)."""
  arr = na.NamedArray('name=S')
  arr.kwappend(name=b'hello')
  arr.kwappend(name=b'world')

  result = arr.get_array('name')
  assert result.tolist() == [b'hello', b'world']


def test_field_extend():
  """NamedArray.extend() appends rows from another NamedArray."""
  arr = na.NamedArray('id=i,name=S,val=f')
  other = na.NamedArray('id=i,name=S,val=f')
  other.kwappend(id=1, name=b'a', val=1.0)
  other.kwappend(id=2, name=b'b', val=2.0)
  arr.extend(other)

  assert arr.get_array('id').tolist() == [1, 2]
  assert arr.get_array('val').tolist() == [1.0, 2.0]


def test_named_array_init_from_string():
  """NamedArray.__init__() parses a 'name=fmt,name=fmt' comma-separated string."""
  arr = na.NamedArray('id=i,name=S,val=f')

  assert len(arr) == 0
  assert tuple(arr.data().keys()) == ('id', 'name', 'val')


def test_named_array_init_from_names_and_fmt():
  """NamedArray.__init__() accepts separate names and format lists."""
  arr = na.NamedArray(['a', 'b', 'c'], fmt='i f d')

  assert len(arr) == 0
  assert tuple(arr.data().keys()) == ('a', 'b', 'c')


def test_named_array_kwappend():
  """NamedArray.kwappend() adds a row of named values."""
  arr = na.NamedArray('id=i,name=S,val=f')
  arr.kwappend(id=1, name=b'alice', val=1.5)
  arr.kwappend(id=2, name=b'bob', val=2.5)

  assert len(arr) == 2
  assert arr.data()['id'].tolist() == [1, 2]
  assert arr.data()['val'].tolist() == [1.5, 2.5]


def test_named_array_get_array():
  """NamedArray.get_array() returns the numpy array for a field."""
  arr = na.NamedArray('x=f,y=i')
  arr.kwappend(x=1.0, y=10)
  arr.kwappend(x=2.0, y=20)

  assert arr.get_array('x').tolist() == [1.0, 2.0]
  assert arr.get_array('y').tolist() == [10, 20]


def test_named_array_to_numpy():
  """NamedArray.to_numpy() returns a 2D array of numeric fields."""
  arr = na.NamedArray('a=f,b=i')
  arr.kwappend(a=1.5, b=10)
  arr.kwappend(a=2.5, b=20)

  result = arr.to_numpy()
  assert result.tolist() == [[1.5, 10], [2.5, 20]]
  assert result.shape == (2, 2)


def test_named_array_to_dataframe():
  """NamedArray.to_dataframe() returns a pandas DataFrame."""
  arr = na.NamedArray('id=i,val=f')
  arr.kwappend(id=1, val=10.0)
  arr.kwappend(id=2, val=20.0)

  df = arr.to_dataframe()
  assert isinstance(df, pd.DataFrame)
  assert list(df.columns) == ['id', 'val']
  assert df['id'].tolist() == [1, 2]
  assert df['val'].tolist() == [10.0, 20.0]


def test_named_array_shape_and_dtypes():
  """NamedArray.shape() and dtypes() report correct metadata."""
  arr = na.NamedArray('a=f,b=i,c=S')
  arr.kwappend(a=1.0, b=2, c=b'abc')

  assert arr.shape == (1, 3)
  dtypes = arr.dtypes()
  assert dtypes[0] == np.float32
  assert dtypes[1] == np.int32
  assert dtypes[2] == np.dtype('O')
