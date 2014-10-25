#!/usr/bin/env python

from setuptools import setup, find_packages
import powerpool


setup(name='powerpool',
      version=powerpool.__version__,
      description='A pluggable mining pool server implementation',
      author='Isaac Cook',
      author_email='isaac@simpload.com',
      url='http://www.python.org/sigs/distutils-sig/',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'pp = powerpool.main:main'
          ]
      }
      )
