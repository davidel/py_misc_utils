import os
import re
import requests
import shutil
import tempfile
import urllib.parse as uparse

from . import alog
from . import assert_checks as tas
from . import file_overwrite as fow
from . import http_headers as hh
from . import utils as ut


def clean_fsname(name):
  name = re.sub(r'[^\w\s\-\._]', '_', name)

  return re.sub(r'_+', '_', name).strip('_')


def get_cache_coords(cache_dir, url):
  purl = uparse.urlparse(url)

  fparts = [clean_fsname(purl.netloc)]
  fparts += [clean_fsname(p) for p in ut.path_split(purl.path)]

  filename = new_filename = fparts[-1]
  if purl.params:
    new_filename += '_' + purl.params
  if purl.query:
    new_filename += '_' + purl.query
  if new_filename != filename:
    fparts[-1] = clean_fsname(new_filename)

  return os.path.join(cache_dir, *fparts), filename


def download(url, path, chunk_size=None):
  chunk_size = chunk_size or 1024 * 1024

  alog.debug(f'Fetching "{url}" to "{path}"')

  req = requests.get(url, stream=True)

  with fow.FileOverwrite(path, mode='wb') as fd:
    while True:
      data = req.raw.read(chunk_size)
      fd.write(data)
      if len(data) < chunk_size:
        break

  return req.headers


STAMP_HEADERS = set(h.lower() for h in
                    (
                      hh.LAST_MODIFIED,
                      hh.ETAG,
                      hh.CONTENT_LENGTH,
                      hh.CONTENT_TYPE,
                    ))

def parse_headers(headers):
  pheaders, keys = [], [(k.lower(), k) for k in headers.keys()]
  for lk, k in sorted(keys):
    v = headers[k]
    if lk in STAMP_HEADERS:
      pheaders.append(f'{lk}: {v}')

  return '\n'.join(pheaders)


def needs_download(url, upath, chpath):
  if not os.path.isfile(upath) or not os.path.isfile(chpath):
    return True

  hreq = requests.head(url)

  alog.debug(f'Remote HEAD headers: {hreq.headers}')

  pheaders = parse_headers(hreq.headers)

  with open(chpath, mode='r') as hfd:
    cheaders = hfd.read()

  return pheaders != cheaders


def fetch(url, dest_path=None, dest_dir=None, cache_dir=None):
  cache_dir = cache_dir or os.path.join(os.getenv('HOME', '.'), '.cache')
  cache_dir = os.path.join(cache_dir, 'http_cache')

  udir, filename = get_cache_coords(cache_dir, url)
  os.makedirs(udir, exist_ok=True)

  upath = os.path.join(udir, 'content')
  chpath = os.path.join(udir, 'cache_headers')

  if needs_download(url, upath, chpath):
    headers = download(url, upath)

    alog.debug(f'Remote GET headers: {headers}')

    with fow.FileOverwrite(chpath, mode='w') as cfd:
      cfd.write(parse_headers(headers))

  if dest_path is not None:
    ut.link_or_copy(upath, dest_path)
    rpath = dest_path
  elif dest_dir is not None:
    rpath = os.path.join(dest_dir, filename)
    ut.link_or_copy(upath, rpath)
  else:
    rpath = upath

  return rpath, filename


class LocalFile:

  def __init__(self, url_or_path, cache_dir=None, uncompress=False):
    self.url_or_path = url_or_path
    self.cache_dir = cache_dir
    self.uncompress = uncompress
    self.tempdir = None

  def __enter__(self):
    if self.url_or_path.startswith('http:') or self.url_or_path.startswith('https:'):
      self.tempdir = tempfile.mkdtemp()

      rpath, _ = fetch(self.url_or_path,
                       dest_dir=self.tempdir,
                       cache_dir=self.cache_dir)

      alog.debug(f'Returning local copy of "{self.url_or_path}" in "{rpath}"')
    else:
      rpath = self.url_or_path

    if self.uncompress:
      bpath, ext = os.path.splitext(rpath)
      if ext in ('.gz', '.bz2', '.bzip'):
        if self.tempdir is None:
          self.tempdir = tempfile.mkdtemp()
          bpath = os.path.join(self.tempdir, os.path.basename(bpath))

        alog.debug(f'Uncompressing "{rpath}" to "{bpath}"')

        if ext == '.gz':
          ut.fgunzip(rpath, bpath)
        else:
          ut.fbunzip2(rpath, bpath)

        shutil.copystat(rpath, bpath)
        rpath = bpath

    return rpath

  def __exit__(self, *exc):
    if self.tempdir:
      shutil.rmtree(self.tempdir, ignore_errors=True)

    return False

