import os

import numpy as np
import pandas as pd
import sklearn.decomposition
import sklearn.neighbors
import sklearn.preprocessing
import sklearn.random_projection
import sklearn.utils.extmath

from . import alog
from . import assert_checks as tas
from . import utils as pyu


class Quantizer:

  def __init__(self, nbins=3, std_k=2.0, eps=1e-6):
    self._nbins = nbins
    self._std_k = std_k
    self._eps = eps
    self._mean = None
    self._std = None

  def fit(self, X, *args):
    self._mean = np.mean(X, axis=0, keepdims=True)
    std = np.std(X, axis=0, keepdims=True)
    self._std = np.where(std > self._eps, std, self._eps)

    return self

  def transform(self, X):
    q = np.round(self._nbins * (X - self._mean) / (self._std_k * self._std))

    return np.clip(q, -self._nbins, self._nbins)

  def fit_transform(self, X, *args):
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
  trs, *spec = trs_spec.split(':', 1)
  spec_cfg = pyu.parse_config(spec[0]) if spec else dict()

  alog.debug0(f'Parsed Transformer: {trs}\t{spec_cfg}')

  trs_fn = _TRANSFORMERS.get(trs)
  tas.check_is_not_none(trs_fn, msg=f'Unknown transformation requested: {trs_spec}')

  return trs_fn(**spec_cfg)


class TransformPipeline:

  def __init__(self, transformers):
    self._transformers = []
    for trs in transformers or []:
      if isinstance(trs, str):
        trs = parse_transform(trs)
      self._transformers.append(trs)

  @property
  def transformers(self):
    return tuple(self._transformers)

  @property
  def supports_partial_fit(self):
    return all([hasattr(trs, 'partial_fit') for trs in self._transformers])

  @property
  def supports_fit(self):
    return len(self._transformers) <= 1

  def add(self, transformer):
    self._transformers.append(transformer)

  def transform(self, x):
    tx = x
    for trs in self._transformers:
      tx = trs.transform(tx)

    return tx

  def fit_transform(self, x):
    tx = x
    for trs in self._transformers:
      tx = trs.fit_transform(tx)

    return tx

  def fit(self, x):
    tas.check(self.supports_fit,
              msg=f'Only pipelines with a single transformer can call fit()')
    for trs in self._transformers:
      trs.fit(x)

    return self

  def partial_fit_transform(self, x):
    tx = x
    for trs in self._transformers:
      trs.partial_fit(tx)
      tx = trs.transform(tx)

    return tx


def fit(m, x, y, **kwargs):
  shape = y.shape
  # Some SciKit Learn models (ie. KNeighborsClassifier/KNeighborsRegressor) although
  # supporting multi-output targets, insist on special casing the 1-output case
  # requiring the 1D (N,) vector instead of a (N, 1) tensor.
  if len(shape) == 2 and shape[-1] == 1:
    y = np.squeeze(pyu.to_numpy(y), axis=-1)

  return m.fit(x, y, **kwargs)


def predict(m, x, **kwargs):
  y = m.predict(x, **kwargs)

  # Some SciKit Learn models (ie. KNeighborsClassifier/KNeighborsRegressor) although
  # supporting multi-output targets, insist on special casing the 1-output case
  # emitting 1D (N,) vectors instead of a (N, 1) tensors.
  return y if y.ndim > 1 else y.reshape(-1, 1)


def predict_proba(m, x, classes=None):

  def extract_probs(p, cls):
    preds = [p[:, c].reshape(-1, 1) for c in cls]
    return np.concatenate(preds, axis=-1)

  probs = m.predict_proba(x)
  if classes is None:
    return probs

  if isinstance(probs, (list, tuple)):
    return [extract_probs(p, classes) for p in probs]

  return extract_probs(probs, classes)


def _get_weights(dist, weights):
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

  def __init__(self, **kwargs):
    self._sample_weight = None
    self._neigh = sklearn.neighbors.KNeighborsClassifier(**kwargs)

  def fit(self, X, y, sample_weight=None):
    self._sample_weight = sample_weight
    self._neigh.fit(X, y)

    return self

  def _prepare_predict(self, X):
    neigh_dist, neigh_ind = self._neigh.kneighbors(X)

    classes = self._neigh.classes_
    y = self._neigh._y
    if not self._neigh.outputs_2d_:
      y = y.reshape(-1, 1)
      classes = [classes]
    cweights = _get_weights(neigh_dist, self._neigh.weights)
    sample_weight = self._sample_weight
    if sample_weight.ndim < y.ndim:
      sample_weight = sample_weight.reshape(-1, 1)

    return pyu.make_object(neigh_dist=neigh_dist,
                           neigh_ind=neigh_ind,
                           y=y,
                           classes=classes,
                           cweights=cweights,
                           sample_weight=sample_weight)

  def predict(self, X):
    if self._sample_weight is None:
      return self._neigh.predict(X)

    pp = self._prepare_predict(X)

    y_pred = np.empty((len(X), len(pp.classes)), dtype=pp.classes[0].dtype)
    for k, k_classes in enumerate(pp.classes):
      weights = pp.sample_weight[pp.neigh_ind, k] * pp.cweights
      mode, _ = sklearn.utils.extmath.weighted_mode(pp.y[pp.neigh_ind, k], weights,
                                                    axis=1)

      mode = np.asarray(mode.ravel(), dtype=np.intp)
      y_pred[:, k] = k_classes.take(mode)

    if not self._neigh.outputs_2d_:
      y_pred = y_pred.ravel()

    return y_pred

  def predict_proba(self, X):
    if self._sample_weight is None:
      return self._neigh.predict_proba(X)

    pp = self._prepare_predict(X)

    all_rows = np.arange(len(X))
    probabilities = []
    for k, k_classes in enumerate(pp.classes):
      pred_labels = pp.y[:, k][pp.neigh_ind]
      proba_k = np.zeros((len(X), k_classes.size))

      ksweight = pp.sample_weight[:, k]
      for i, idx in enumerate(pred_labels.T):
        proba_k[all_rows, idx] += ksweight * pp.cweights[:, i]

      normalizer = proba_k.sum(axis=1)[:, np.newaxis]
      normalizer[normalizer == 0.0] = 1.0

      probabilities.append(proba_k / normalizer)

    if not self._neigh.outputs_2d_:
      probabilities = probabilities[0]

    return probabilities

