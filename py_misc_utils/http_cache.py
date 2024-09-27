import os
import re
import requests
import shutil
import tempfile
import urllib.parse as uparse

from . import alog
from . import assert_checks as tas
from . import file_overwrite as fow
from . import utils as ut


def clean_fsname(name):
  name = re.sub(r'[^\w\s\-\._]', '_', name)

  return re.sub(r'_+', '_', name).strip('_')


def get_cache_coords(cache_dir, url):
  purl = uparse.urlparse(url)

  fparts = [clean_fsname(purl.netloc)]
  fparts += [clean_fsname(p) for p in ut.path_split(purl.path)]

  filename = parts.pop()
  if purl.params:
    filename += '_' + clean_fsname(purl.params)
  if purl.query:
    filename += '_' + clean_fsname(purl.query)

  return os.path.join(cache_dir, *fparts), filename


def download(url, path, chunk_size=None):
  chunk_size = chunk_size or 1024 * 1024

  alog.debug(f'Fetching "{url}" to "{path}"')

  req = requests.get(url, stream=True)

  with fow.FileOverwrite(path, mode='wb') as fd:
    for data in req.iter_content(chunk_size=chunk_size):
      fd.write(data)

  return req.headers


STAMP_HEADERS = {'last-modified', 'etag', 'content-length', 'content-type'}

def parse_headers(headers):
  pheaders, keys = [], [(k.lower(), k) for k in headers.keys()]
  for lk, k in sorted(keys):
    v = headers[k]
    if lk in STAMP_HEADERS:
      pheaders.append(f'({lk}: {v})')

  return ','.join(pheaders)


def needs_download(url, upath, chpath):
  if not os.path.isfile(upath) or not os.path.isfile(chpath):
    return True

  hreq = requests.head(url)

  alog.debug(f'Remote HEAD headers: {hreq.headers}')

  pheaders = parse_headers(hreq.headers)

  with open(chpath, mode='rb') as hfd:
    cheaders = hfd.read()

  return pheaders != cheaders


def fetch(url, dest_path=None, cache_dir=None):
  cache_dir = cache_dir or os.path.join(os.getenv('HOME', '.'), '.cache', 'http_cache')

  udir, ufile = get_cache_coords(cache_dir, url)
  os.makedirs(udir, exist_ok=True)

  upath = os.path.join(udir, ufile)
  chpath = os.path.join(udir, 'cache_headers')

  if needs_download(url, upath, chpath):
    headers = download(url, upath)

    alog.debug(f'Remote GET headers: {headers}')

    with fow.FileOverwrite(chpath, mode='wb') as cfd:
      cfd.write(parse_headers(headers))

  if dest_path is not None:
    shutil.copyfile(upath, dest_path)

  return upath


class LocalFile:

  def __init__(self, url_or_path, cache_dir=None):
    self.url_or_path = url_or_path
    self.cache_dir = cache_dir

  def __enter__(self):
    if self.url_or_path.startswith('http:') or self.url_or_path.startswith('https:'):
      return fetch(self.url_or_path, cache_dir=self.cache_dir)
    else:
      return self.url_or_path

  def __exit__(self, *exc):
    return False

