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


def test_weighed_knn_predict_proba_with_sample_weight():
  """WeighedKNNClassifier.predict_proba() applies sample weights to the neighbor votes."""
  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([0, 1, 1, 2])
  sw = np.array([1.0, 0.01, 0.01, 1.0])

  clf = sku.WeighedKNNClassifier(n_neighbors=3)
  clf.fit(X, y, sample_weight=sw)

  # Single-row prediction: the two low-weight class-1 samples hardly vote,
  # so the class-0 neighbor dominates (crashed before the fix).
  probs = clf.predict_proba(np.array([[1.5]]))
  assert probs.shape == (1, 3)
  assert probs[0, 0] > 0.98

  # The neighbor votes must be routed through the neighbor labels:
  # compare with a brute-force aggregation of sample_weight * distance
  # weights over the neighbor indices.
  Xq = np.array([[1.5], [2.4]])
  probs = clf.predict_proba(Xq)
  assert probs.shape == (2, 3)
  assert np.allclose(probs.sum(axis=1), 1.0)

  neigh_dist, neigh_ind = clf._neigh.kneighbors(Xq)
  dist_w = sku._get_weights(neigh_dist, clf._neigh.weights)
  classes = clf._neigh.classes_
  y_train = clf._neigh._y.reshape(-1)
  for r in range(len(Xq)):
    votes = np.zeros(classes.size)
    for i, idx in enumerate(neigh_ind[r]):
      votes[classes == y_train[idx]] += sw[idx] * dist_w[r, i]
    votes /= votes.sum()
    assert np.allclose(probs[r], votes)

  # The probability argmax matches predict().
  assert np.array_equal(probs.argmax(axis=1), clf.predict(Xq))


def test_weighed_knn_predict_proba_matches_sklearn_with_ones_weight():
  """WeighedKNNClassifier.predict_proba() with all-ones weights matches sklearn."""
  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([0, 1, 0, 1])
  Xq = np.array([[0.2], [1.5], [2.9], [3.0]])

  for n_neighbors in (1, 2, 3, 4):
    for weights in ("uniform", "distance"):
      ref = sklearn.neighbors.KNeighborsClassifier(n_neighbors=n_neighbors,
                                                   weights=weights)
      ref.fit(X, y)

      clf = sku.WeighedKNNClassifier(n_neighbors=n_neighbors, weights=weights)
      clf.fit(X, y, sample_weight=np.ones(len(X)))

      assert np.allclose(clf.predict_proba(Xq), ref.predict_proba(Xq))


def test_weighed_knn_multioutput_sample_weight():
  """WeighedKNNClassifier handles multi-output targets with sample_weight."""
  X = np.array([[0.0], [1.0], [2.0], [3.0]])
  y = np.array([[0, 1], [1, 1], [1, 0], [2, 0]])
  Xq = np.array([[1.5], [2.4]])

  ones_clf = sku.WeighedKNNClassifier(n_neighbors=3)
  ones_clf.fit(X, y, sample_weight=np.ones(len(X)))
  ref = sklearn.neighbors.KNeighborsClassifier(n_neighbors=3)
  ref.fit(X, y)
  ref_probs = ref.predict_proba(Xq)

  probs = ones_clf.predict_proba(Xq)
  assert len(probs) == 2
  for k, probs_k in enumerate(probs):
    assert np.allclose(probs_k, ref_probs[k])

  clf = sku.WeighedKNNClassifier(n_neighbors=3)
  clf.fit(X, y, sample_weight=np.array([1.0, 0.01, 0.01, 1.0]))
  # No IndexError on the second output (crashed before the fix).
  preds = clf.predict(Xq)
  assert preds.shape == (2, 2)
  proba = clf.predict_proba(Xq)
  assert [p.shape for p in proba] == [(2, 3), (2, 2)]
