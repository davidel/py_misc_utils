"""Tests for py_misc_utils.skl_utils.

Requires scikit-learn. Uses pytest.importorskip("sklearn") so the file
is skipped when sklearn is not installed.
"""

import numpy as np
import pytest

sklearn = pytest.importorskip("sklearn")

from py_misc_utils import skl_utils as sku


def test_quantizer_fit_transform():
  """Quantizer.fit_transform() scales data into integer-valued bins."""
  X = np.array([[0.0, 1.0], [2.0, 3.0], [4.0, 5.0]])
  q = sku.Quantizer(nbins=3, std_k=1.0, eps=1e-6)
  result = q.fit_transform(X)

  assert result.shape == X.shape
  assert np.all(np.equal(np.mod(result, 1), 0))  # integer-valued
  assert np.all(result >= -3)
  assert np.all(result <= 3)


def test_quantizer_fit_then_transform():
  """Quantizer.fit() then transform() produces same result as fit_transform()."""
  X = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
  q1 = sku.Quantizer(nbins=2, std_k=1.0)
  q1.fit(X)
  r1 = q1.transform(X)

  q2 = sku.Quantizer(nbins=2, std_k=1.0)
  r2 = q2.fit_transform(X)

  assert np.array_equal(r1, r2)


def test_quantizer_transform_bins():
  """Quantizer bins data into [-nbins, nbins] by fit/transform behavior."""
  q = sku.Quantizer(nbins=5, std_k=3.0, eps=1e-8)

  X = np.array([[0.0, 100.0], [1.0, 104.0], [2.0, 96.0]])
  q.fit(X)
  out = q.transform(X)

  assert out.shape == X.shape
  # Quantized values are integers within [-nbins, nbins].
  assert np.all(out >= -5) and np.all(out <= 5)
  # Round-trip: transform preserves the order of the input values.
  assert np.array_equal(np.argsort(out[:, 0]), np.argsort(X[:, 0]))


def test_parse_transform_known_name():
  """parse_transform() returns a Quantizer for 'QTZ'."""
  tr = sku.parse_transform('QTZ')

  assert isinstance(tr, sku.Quantizer)


def test_parse_transform_with_config():
  """parse_transform('PCA:{n_components: 2}') returns PCA with n_components=2."""
  tr = sku.parse_transform('PCA:{n_components: 2}')

  import sklearn.decomposition
  assert isinstance(tr, sklearn.decomposition.PCA)
  assert tr.n_components == 2


def test_parse_transform_unknown_name():
  """parse_transform() raises for unknown transformer names."""
  with pytest.raises(AssertionError):
    sku.parse_transform('NOT_A_TRANSFORMER')


def test_transform_pipeline_fit_transform():
  """TransformPipeline.fit_transform() fits then transforms."""
  X = np.array([[1.0, 2.0], [3.0, 4.0]])
  pipe = sku.TransformPipeline(['QTZ'])

  result = pipe.fit_transform(X)

  assert result.shape == X.shape


def test_fit_normalizes_2d_single_output():
  """sku.fit() squeezes (N, 1) targets to (N,) for single-output models."""
  from sklearn.neighbors import KNeighborsRegressor

  X = np.array([[1.0], [2.0], [3.0]])
  y = np.array([[1.0], [2.0], [3.0]])

  model = sku.fit(KNeighborsRegressor(n_neighbors=1), X, y)

  assert model.predict(X).shape == (3,)


def test_predict_returns_2d():
  """sku.predict() reshapes 1D predictions to (N, 1)."""
  from sklearn.neighbors import KNeighborsRegressor

  X = np.array([[1.0], [2.0], [3.0]])
  y = np.array([1.0, 2.0, 3.0])

  model = sku.fit(KNeighborsRegressor(n_neighbors=1), X, y)
  result = sku.predict(model, X)

  assert result.ndim == 2
  assert result.shape == (3, 1)


def test_predict_proba_classes():
  """sku.predict_proba() extracts requested class columns when classes given."""
  from sklearn.neighbors import KNeighborsClassifier

  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([0, 1, 0, 1])

  model = sku.fit(KNeighborsClassifier(n_neighbors=2), X, y)
  probs = sku.predict_proba(model, X, classes=[0])

  assert probs.shape == (4, 1)


def test_weighed_knn_without_sample_weight():
  """WeighedKNNClassifier without sample_weight delegates to KNeighborsClassifier."""
  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([0, 1, 0, 1])

  clf = sku.WeighedKNNClassifier(n_neighbors=2)
  clf.fit(X, y)

  preds = clf.predict(X)
  assert preds.shape == (4,)


def test_weighed_knn_with_sample_weight():
  """WeighedKNNClassifier with sample_weight produces weighted predictions."""
  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([0, 1, 0, 1])
  sw = np.array([1.0, 10.0, 1.0, 1.0])

  clf = sku.WeighedKNNClassifier(n_neighbors=2)
  clf.fit(X, y, sample_weight=sw)

  preds = clf.predict(X)
  assert preds.shape == (4,)


def test_weighed_knn_predict_proba():
  """WeighedKNNClassifier.predict_proba() returns probabilities for each class."""
  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([0, 1, 0, 1])

  clf = sku.WeighedKNNClassifier(n_neighbors=2)
  clf.fit(X, y)

  probs = clf.predict_proba(X)
  assert probs.shape == (4, 2)
