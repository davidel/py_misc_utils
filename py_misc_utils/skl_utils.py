"""SciKit-Learn utilities for preprocessing pipelines and weighted KNN.

This module provides transformer factories (e.g. :class:`Quantizer`),
:func:`parse_transform` for spec-based transformer construction, and
:class:`TransformPipeline` for composing transformers. It also provides
:class:`WeighedKNNClassifier` implementing sample-weighted KNN
classification/probability estimation, plus fit/predict helpers handling
single- vs multi-output target shapes.
"""

import numpy as np
import sklearn.decomposition
import sklearn.neighbors
import sklearn.preprocessing
import sklearn.random_projection
import sklearn.utils.extmath

from . import alog
from . import assert_checks as tas
from . import utils as pyu


class Quantizer:
  """Uniform quantization transformer scaling data into symmetric integer bins.

  Normalizes data by mean and std_k*std, rounds to integer bins, and clips
  to the range [-nbins, nbins].

  Example:
    import numpy as np

    q = skl_utils.Quantizer(nbins=2)
    X = np.array([[1.0], [2.0], [3.0], [100.0]])
    q.fit(X)
    q.transform(X)  # Integer values in [-2, 2].
  """

  def __init__(self, nbins=3, std_k=2.0, eps=1e-6):
    """Initialize the Quantizer.

    Args:
      nbins: Number of bins on each side of zero (default: 3).
      std_k: Std multiplier used for scaling (default: 2.0).
      eps: Minimum std value to avoid division by zero (default: 1e-6).
    """
    self._nbins = nbins
    self._std_k = std_k
    self._eps = eps
    self._mean = None
    self._std = None

  def fit(self, X, *args):
    """Compute column means and stds from X.

    Args:
      X: Input data array.
      *args: Ignored; present for sklearn transformer compatibility.

    Returns:
      The fitted Quantizer instance.
    """
    self._mean = np.mean(X, axis=0, keepdims=True)
    std = np.std(X, axis=0, keepdims=True)
    self._std = np.where(std > self._eps, std, self._eps)

    return self

  def transform(self, X):
    """Quantize X into integer bins in [-nbins, nbins].

    Args:
      X: Input data array.

    Returns:
      The quantized array.
    """
    q = np.round(self._nbins * (X - self._mean) / (self._std_k * self._std))

    return np.clip(q, -self._nbins, self._nbins)

  def fit_transform(self, X, *args):
    """Fit the Quantizer on X and return the transformed data.

    Args:
      X: Input data array.
      *args: Ignored; present for sklearn transformer compatibility.

    Returns:
      The quantized array.
    """
    return self.fit(X).transform(X)


_TRANSFORMERS = {
    'QTZ': Quantizer,
    'STD_SCALE': sklearn.preprocessing.StandardScaler,
    'MIN_MAX': sklearn.preprocessing.MinMaxScaler,
    'MAX_ABS': sklearn.preprocessing.MaxAbsScaler,
    'ROBUST': sklearn.preprocessing.RobustScaler,
    'QUANT': sklearn.preprocessing.QuantileTransformer,
    'POWER': sklearn.preprocessing.PowerTransformer,
    'NORM': sklearn.preprocessing.Normalizer,
    'PCA': sklearn.decomposition.PCA,
    'FICA': sklearn.decomposition.FastICA,
    'INCPCA': sklearn.decomposition.IncrementalPCA,
    'KBINDIS': sklearn.preprocessing.KBinsDiscretizer,
    'POLYF': sklearn.preprocessing.PolynomialFeatures,
    'PWR': sklearn.preprocessing.PowerTransformer,
    'SPLN': sklearn.preprocessing.SplineTransformer,
    'GRPRJ': sklearn.random_projection.GaussianRandomProjection,
    'SRPRJ': sklearn.random_projection.SparseRandomProjection,
}


def parse_transform(trs_spec):
  """Create a transformer instance from a spec string.

  The spec format is 'NAME' or 'NAME:cfg', where cfg is a config string
  parsed via pyu.parse_config (a YAML dict such as '{nbins: 4}', or a
  path to a YAML file).

  Args:
    trs_spec: The transformer spec string (e.g. 'QTZ', 'PCA:{n_components: 2}').

  Returns:
    A configured transformer instance.

  Raises:
    AssertionError: When the transformer name is unknown.

  Example:
    skl_utils.parse_transform('QTZ:{nbins: 4}')  # Quantizer(nbins=4)
    skl_utils.parse_transform('PCA')  # sklearn.decomposition.PCA()
  """
  trs, *spec = trs_spec.split(':', 1)
  spec_cfg = pyu.parse_config(spec[0]) if spec else dict()

  alog.debug0(f'Parsed Transformer: {trs}\t{spec_cfg}')

  trs_fn = _TRANSFORMERS.get(trs)
  tas.check_is_not_none(trs_fn, msg=f'Unknown transformation requested: {trs_spec}')

  return trs_fn(**spec_cfg)


class TransformPipeline:
  """A sequence of transformers applied in order to input data.

  Supports adding transformers, partial_fit/full-fit, and transform.
  Transformers can be given as instances or spec strings: strings passed
  to the constructor are parsed via parse_transform(), while add() keeps
  them as-is.

  Example:
    tp = skl_utils.TransformPipeline(['QTZ', 'PCA:{n_components: 1}'])
    import numpy as np
    tp.fit_transform(np.random.rand(8, 4))
  """

  def __init__(self, transformers):
    """Initialize the pipeline from a list of transformers or spec strings.

    Args:
      transformers: A list of transformer instances or spec strings.
    """
    self._transformers = []
    for trs in transformers or []:
      if isinstance(trs, str):
        trs = parse_transform(trs)
      self._transformers.append(trs)

  @property
  def transformers(self):
    """Return the list of transformers in the pipeline.

    Returns:
      A tuple of transformer instances.
    """
    return tuple(self._transformers)

  @property
  def supports_partial_fit(self):
    """Whether all transformers support partial_fit.

    Returns:
      True if every transformer has a partial_fit method.
    """
    return all(hasattr(trs, 'partial_fit') for trs in self._transformers)

  @property
  def supports_fit(self):
    """Whether the pipeline supports full fit.

    Returns:
      True if the pipeline has at most one transformer (fit is not
      distributed correctly across multiple transformers).
    """
    return len(self._transformers) <= 1

  def add(self, transformer):
    """Append a transformer to the pipeline.

    Unlike the constructor, spec strings are kept as-is instead of being
    parsed via parse_transform().

    Args:
      transformer: The transformer instance or spec string to add.
    """
    self._transformers.append(transformer)

  def transform(self, x):
    """Apply all transformers in order to x.

    Args:
      x: Input data array.

    Returns:
      The transformed data.
    """
    tx = x
    for trs in self._transformers:
      tx = trs.transform(tx)

    return tx

  def fit_transform(self, x):
    """Fit and transform x through all transformers in order.

    Args:
      x: Input data array.

    Returns:
      The transformed data.
    """
    tx = x
    for trs in self._transformers:
      tx = trs.fit_transform(tx)

    return tx

  def fit(self, x):
    """Fit all transformers on x (only valid for single-transformer pipelines).

    Args:
      x: Input data array.

    Returns:
      The pipeline itself.
    """
    tas.check(self.supports_fit,
              msg='Only pipelines with a single transformer can call fit()')
    for trs in self._transformers:
      trs.fit(x)

    return self

  def partial_fit_transform(self, x):
    """Partial-fit all transformers on x and return the transformed data.

    Args:
      x: Input data array.

    Returns:
      The transformed data.
    """
    tx = x
    for trs in self._transformers:
      trs.partial_fit(tx)
      tx = trs.transform(tx)

    return tx


def fit(m, x, y, **kwargs):
  """Fit a model, normalizing single-output 2D targets to 1D.

  Some sklearn models (e.g. KNeighborsClassifier) require 1D (N,) targets
  for single-output problems rather than (N, 1) tensors.

  Args:
    m: The model to fit.
    x: Input features.
    y: Target values.
    **kwargs: Additional keyword arguments passed to m.fit.

  Returns:
    The fitted model.

  Example:
    import numpy as np
    import sklearn.linear_model

    model = sklearn.linear_model.LinearRegression()
    x = np.array([[1.0], [2.0], [3.0]])
    y = np.array([[1.0], [2.0], [3.0]])
    skl_utils.fit(model, x, y)  # (N, 1) y is handled.
  """
  shape = y.shape
  # Some SciKit Learn models (ie. KNeighborsClassifier/KNeighborsRegressor) although
  # supporting multi-output targets, insist on special casing the 1-output case
  # requiring the 1D (N,) vector instead of a (N, 1) tensor.
  if len(shape) == 2 and shape[-1] == 1:
    y = np.squeeze(np.asarray(y), axis=-1)

  return m.fit(x, y, **kwargs)


def predict(m, x, **kwargs):
  """Predict targets, normalizing single-output results to 2D.

  Some sklearn models emit 1D (N,) vectors for single-output problems;
  this reshapes them to (N, 1) for consistency.

  Args:
    m: The fitted model.
    x: Input features.
    **kwargs: Additional keyword arguments passed to m.predict.

  Returns:
    The predicted targets, always with ndim >= 2.

  Example:
    import numpy as np
    import sklearn.linear_model

    model = sklearn.linear_model.LinearRegression()
    x = np.array([[1.0], [2.0], [3.0]])
    skl_utils.fit(model, x, np.array([[1.0], [2.0], [3.0]]))
    skl_utils.predict(model, x).shape  # (3, 1) for single-output models.
  """
  y = m.predict(x, **kwargs)

  # Some SciKit Learn models (ie. KNeighborsClassifier/KNeighborsRegressor) although
  # supporting multi-output targets, insist on special casing the 1-output case
  # emitting 1D (N,) vectors instead of a (N, 1) tensors.
  return y if y.ndim > 1 else y.reshape(-1, 1)


def predict_proba(m, x, classes=None):
  """Predict class probabilities, optionally extracting specific classes.

  Args:
    m: The fitted model.
    x: Input features.
    classes: Optional list of class indices to extract per output. If None,
      return the model's probabilities unchanged.

  Returns:
    The probability array(s), filtered to requested classes if provided.

  Example:
    import numpy as np
    import sklearn.neighbors

    model = sklearn.neighbors.KNeighborsClassifier(n_neighbors=1)
    x = np.array([[0.0], [1.0], [10.0], [11.0]])
    skl_utils.fit(model, x, np.array([0, 0, 1, 1]))
    skl_utils.predict_proba(model, np.array([[0.5]]))  # All the class probabilities.
    skl_utils.predict_proba(model, np.array([[0.5]]), classes=[1])  # Only class 1.
  """

  def extract_probs(p, cls):
    """Extract probability columns for requested class indices.

    Args:
      p: The probability array.
      cls: A list of class indices.

    Returns:
      The concatenated probability columns.
    """
    preds = [p[:, c].reshape(-1, 1) for c in cls]
    return np.concatenate(preds, axis=-1)

  probs = m.predict_proba(x)
  if classes is None:
    return probs

  if isinstance(probs, (list, tuple)):
    return [extract_probs(p, classes) for p in probs]

  return extract_probs(probs, classes)


def _get_weights(dist, weights):
  """Compute neighbor weights from distances, as per the sklearn policy.

  Args:
    dist: 2D array of distances between each sample and its neighbors.
    weights: Weighting policy: None or 'uniform' for equal weights,
      'distance' for inverse-distance weights, or a callable taking the
      distances and returning the weights.

  Returns:
    The array of weights for the neighbors.

  Raises:
    ValueError: When the policy is not recognized.
  """
  if weights in (None, "uniform"):
    return np.ones_like(dist)
  elif weights == "distance":
    # if user attempts to classify a point that was zero distance from one
    # or more training points, those training points are weighted as 1.0
    # and the other points as 0.0
    if dist.dtype is np.dtype:
      for point_dist_i, point_dist in enumerate(dist):
        # check if point_dist is iterable
        # (ex: RadiusNeighborClassifier.predict may set an element of
        # dist to 1e-6 to represent an 'outlier')
        if hasattr(point_dist, "__contains__") and 0.0 in point_dist:
          dist[point_dist_i] = point_dist == 0.0
        else:
          dist[point_dist_i] = 1.0 / point_dist
    else:
      with np.errstate(divide="ignore"):
        dist = 1.0 / dist
      inf_mask = np.isinf(dist)
      inf_row = np.any(inf_mask, axis=1)
      dist[inf_row] = inf_mask[inf_row]
      return dist
  elif callable(weights):
    return weights(dist)
  else:
    alog.xraise(ValueError, f'Unrecognized "weights" value: {weights}')


class WeighedKNNClassifier:
  """KNN classifier supporting per-sample weights in the neighborhood vote.

  When no sample weights are provided the operations are simply delegated
  to sklearn.neighbors.KNeighborsClassifier; otherwise the weighted mode
  of the neighbor labels drives the predictions: each neighbor votes with
  a weight of sample_weight * neighbor-distance-weight.

  Args:
    **kwargs: Keyword arguments forwarded to KNeighborsClassifier.

  Example:
    import numpy as np

    knn = skl_utils.WeighedKNNClassifier(n_neighbors=3)
    X = np.array([[0.0], [1.0], [2.0], [3.0]])
    y = np.array([0, 1, 1, 2])
    weights = np.array([1.0, 0.01, 0.01, 1.0])
    knn.fit(X, y, sample_weight=weights)
    knn.predict(np.array([[1.6]]))
  """

  def __init__(self, **kwargs):
    """Initialize the classifier, wrapping a KNeighborsClassifier.

    Args:
      **kwargs: Keyword arguments forwarded to KNeighborsClassifier.
    """
    # Per-sample weights provided at fit time, when any.
    self._sample_weight = None
    self._neigh = sklearn.neighbors.KNeighborsClassifier(**kwargs)

  def fit(self, X, y, sample_weight=None):
    """Fit the classifier, recording the per-sample weights.

    Args:
      X: Input features.
      y: Target values.
      sample_weight: Optional per-sample weights used to weight the
        neighbors during prediction.

    Returns:
      The fitted classifier.
    """
    self._sample_weight = sample_weight
    self._neigh.fit(X, y)

    return self

  def _prepare_predict(self, X):
    """Collect the prediction inputs of the weighted vote.

    Args:
      X: Input features.

    Returns:
      A namespace object with the neighbor distances and indices, the
      target values and classes normalized to 2D, the neighbor weights
      and the per-sample weights aligned to the targets.
    """
    neigh_dist, neigh_ind = self._neigh.kneighbors(X)

    classes = self._neigh.classes_
    y = self._neigh._y
    if not self._neigh.outputs_2d_:
      y = y.reshape(-1, 1)
      classes = [classes]
    cweights = _get_weights(neigh_dist, self._neigh.weights)
    sample_weight = self._sample_weight
    if sample_weight.ndim < y.ndim:
      sample_weight = np.broadcast_to(sample_weight[:, np.newaxis],
                                      (sample_weight.shape[0], y.shape[1]))

    return pyu.make_object(neigh_dist=neigh_dist,
                           neigh_ind=neigh_ind,
                           y=y,
                           classes=classes,
                           cweights=cweights,
                           sample_weight=sample_weight)

  def predict(self, X):
    """Predict the class labels of the samples.

    Args:
      X: Input features.

    Returns:
      The predicted labels, weighted by the sample weights when they
      were provided at fit time.
    """
    if self._sample_weight is None:
      return self._neigh.predict(X)

    pp = self._prepare_predict(X)

    y_pred = np.empty((len(X), len(pp.classes)), dtype=pp.classes[0].dtype)
    for k, k_classes in enumerate(pp.classes):
      weights = pp.sample_weight[pp.neigh_ind, k] * pp.cweights
      mode, _ = sklearn.utils.extmath.weighted_mode(pp.y[pp.neigh_ind, k],
                                                    weights,
                                                    axis=1)

      mode = np.asarray(mode.ravel(), dtype=np.intp)
      y_pred[:, k] = k_classes.take(mode)

    if not self._neigh.outputs_2d_:
      y_pred = y_pred.ravel()

    return y_pred

  def predict_proba(self, X):
    """Predict the class probabilities of the samples.

    Args:
      X: Input features.

    Returns:
      The class probabilities accumulated from the weighted neighbor
      votes when sample weights were provided at fit time, or the
      probabilities of the wrapped classifier otherwise.
    """
    if self._sample_weight is None:
      return self._neigh.predict_proba(X)

    pp = self._prepare_predict(X)

    all_rows = np.arange(len(X))
    probabilities = []
    for k, k_classes in enumerate(pp.classes):
      pred_labels = pp.y[:, k][pp.neigh_ind]
      proba_k = np.zeros((len(X), k_classes.size))

      ksweight = pp.sample_weight[pp.neigh_ind, k]
      for i, idx in enumerate(pred_labels.T):
        proba_k[all_rows, idx] += ksweight[:, i] * pp.cweights[:, i]

      normalizer = proba_k.sum(axis=1)[:, np.newaxis]
      normalizer[normalizer == 0.0] = 1.0

      probabilities.append(proba_k / normalizer)

    if not self._neigh.outputs_2d_:
      probabilities = probabilities[0]

    return probabilities
