#!/usr/bin/env python

from setuptools import setup, find_packages
from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt")
reqs = [str(ir.req) for ir in install_reqs]


setup(name='powerpool',
      version='0.1',
      description='A pluggable mining pool server implementation',
      author='Isaac Cook',
      author_email='isaac@simpload.com',
      url='http://www.python.org/sigs/distutils-sig/',
      packages=find_packages(),
      install_requires=reqs,
      entry_points={
          'console_scripts': [
              'pp = powerpool.manager:main'
          ]
      }
      )
