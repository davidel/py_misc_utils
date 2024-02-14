import os

import numpy as np
import pandas as pd
import sklearn.decomposition
import sklearn.preprocessing
import sklearn.random_projection

from . import alog
from . import assert_checks as tas
from . import utils as pyu


class Quantizer(object):

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

  trs_fn = _TRANSFORMERS.get(trs, None)
  tas.check_is_not_none(trs_fn, msg=f'Unknown transformation requested: {trs_spec}')

  return trs_fn(**spec_cfg)


class TransformPipeline(object):

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

