#!/usr/bin/env python3

from setuptools import setup, find_packages


setup(name='py_misc_utils',
      version='0.1.27',
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
          'pyyaml',
          'numpy',
          'pandas',
          'psutil',
      ],
      )

