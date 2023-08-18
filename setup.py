# from setuptools import setup, find_packages

# setup(
#     name='Pre-processing-CICD',
#     version='1',
#     description='Project Pre-processing-CICD',
#     include_package_data=True,
#     zip_safe=False,
#     packages=find_packages(exclude=('tests', 'docs', 'sample'))
# )

from distutils.core import setup

setup(
    name='Pre-processing-CICD',
    version='1',
    description='Project Pre-processing-CICD',
    include_package_data=False,
    packages=['.']
)