#!/usr/bin/env python3

from setuptools import setup, find_packages


setup(name='py_misc_utils',
      version='0.1.50',
      description='Miscellaneous Utility APIs',
      author='Davide Libenzi',
      packages=find_packages(),
      package_data={
          'py_misc_utils': [
              # Paths from py_misc_utils/ subfolder ...
          ],
      },
      include_package_data=True,
      install_requires=[
          'numpy',
          'pandas',
          'psutil',
          'pyyaml',
      ],
      extras_require={
          'fs': [
              'boto3',
              'bs4',
              'ftputil',
              'google-cloud-storage',
              'pyarrow',
          ],
      },
      )

