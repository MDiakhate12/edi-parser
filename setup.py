# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='Pre-processing-CICD',
    version='1',
    description='Project Pre-processing-CICD',
    long_description=readme,
    author='Joyce MARAZANO',
    author_email='EXT.JMARAZANO@cma-cgm.com',
    url='my.git.url',
    license=license,
    zip_safe=False,
    packages=find_packages('test-zip'),
    packages_dir  ={'': 'test-zip'}
)
