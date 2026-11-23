"""FileSystem backends for remote and local storage schemes.

This package groups the FileSystem implementations registered in the
FILE_SYSTEMS tuple of every submodule, so that the generic open/copy/list
helpers can select a backend by URL scheme:
- file: local files (fs.file_fs.FileFs)
- ftp: FTP servers (fs.ftp_fs.FtpFs)
- http/https: web servers exposing directory listings (fs.http_fs.HttpFs)
- gs: Google Cloud Storage (fs.gcs_fs.GcsFs)
- s3: Amazon S3 (fs.s3_fs.S3Fs)
"""
