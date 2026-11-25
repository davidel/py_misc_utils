"""Tests for py_misc_utils.np_ml_framework.

Framework-agnostic array adapter for ML libraries. Uses
pytest.importorskip for optional frameworks (torch, jax, tf).
"""

import numpy as np
import pytest

torch = pytest.importorskip("torch")

from py_misc_utils import np_ml_framework as nf


def test_resolve_numpy_only():
  """resolve() with numpy arrays returns numpy module and same arrays."""
  a = np.array([1.0, 2.0, 3.0])
  mod, args = nf.resolve(a)

  assert mod.__npml_name == 'np'
  assert len(args) == 1
  assert np.array_equal(args[0], a)


def test_resolve_torch_only():
  """resolve() with torch tensors returns torch module and same tensors."""
  a = torch.tensor([1.0, 2.0, 3.0])
  mod, args = nf.resolve(a)

  assert mod.__npml_name == 'torch'
  assert len(args) == 1
  assert torch.equal(args[0], a)


def test_resolve_mixed_converts_to_torch():
  """resolve() with mixed numpy+torch converts numpy to torch (higher priority)."""
  a = np.array([1.0, 2.0])
  b = torch.tensor([3.0, 4.0])
  mod, args = nf.resolve(a, b)

  assert mod.__npml_name == 'torch'
  assert isinstance(args[0], torch.Tensor)
  assert torch.equal(args[0], torch.tensor([1.0, 2.0]))
  assert torch.equal(args[1], b)


def test_resolve_empty_args():
  """resolve() with no args returns default module and empty tuple."""
  mod, args = nf.resolve()

  assert mod.__npml_name == 'np'
  assert args == ()


def test_resolve_multiple_numpy():
  """resolve() with multiple numpy arrays returns them unchanged."""
  a = np.array([1.0, 2.0])
  b = np.array([3.0, 4.0])
  mod, args = nf.resolve(a, b)

  assert mod.__npml_name == 'np'
  assert len(args) == 2
  assert np.array_equal(args[0], a)
  assert np.array_equal(args[1], b)


def test_resolve_preserves_numpy_value():
  """resolve() converts numpy values through to_numpy / asarray correctly."""
  a = np.array([[1, 2], [3, 4]])
  mod, args = nf.resolve(a)

  assert mod.__npml_name == 'np'
  assert np.array_equal(args[0], [[1, 2], [3, 4]])


def test_resolve_identifies_torch_tensor():
  """resolve() with only torch tensors selects the torch module."""
  a = torch.tensor([1.0, 2.0])

  mod, args = nf.resolve(a)

  assert mod.__npml_name == 'torch'
  assert args[0] is a


def test_resolve_picks_numpy_for_numpy_only():
  """resolve() with only numpy arrays selects the numpy module."""
  a = np.array([1.0, 2.0])

  mod, args = nf.resolve(a)

  assert mod.__npml_name == 'np'
  assert args[0] is a


def test_resolve_numpy_to_torch_conversion():
  """resolve() converts numpy array to torch tensor when mixed with torch."""
  a = np.array([5.0, 6.0])
  b = torch.tensor([1.0, 2.0])
  mod, args = nf.resolve(a, b)

  assert mod.__npml_name == 'torch'
  converted = args[0]
  assert isinstance(converted, torch.Tensor)
  assert torch.equal(converted, torch.tensor([5.0, 6.0]))
