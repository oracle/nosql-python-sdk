"""
Setup script for the Python driver for Oracle NoSQL Database
"""

import io
import os
import re
from setuptools import setup, find_packages


def open_relative(*path):
    """
    Opens files in read-only with a fixed utf-8 encoding.

    All locations are relative to this setup.py file.

    """
    here = os.path.abspath(os.path.dirname(__file__))
    filename = os.path.join(here, *path)
    return io.open(filename, mode='r', encoding='utf-8')


with open_relative('src', 'borneo', 'version.py') as fd:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        fd.read(), re.MULTILINE).group(1)
    if not version:
        raise RuntimeError('Cannot find version information')

with open_relative('README.rst') as f:
    readme = f.read()

requires = [
    'python-dateutil>=2.7.0',
    'requests>=2.12.0'
    # don't install oci by default, it's only used for the cloud
    # 'oci>=2.2.18'
]

setup(
    name='borneo',
    url='https://nosql-python-sdk.readthedocs.io/en/stable/index.html',

    # Version should match the system release, but may vary as patches
    # are created.
    version=version,
    description='Oracle NoSQL Database Python SDK',
    long_description=readme,
    long_description_content_type='text/x-rst',

    author='Oracle',
    author_email='george.feinberg@oracle.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,

    # License is UPL, Version 1.0
    license='Universal Permissive License 1.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',

        # License -- must match "license" above
        'License :: OSI Approved :: Universal Permissive License (UPL)',

        # Supported Python versions
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],

    # What does your project relate to?
    keywords='database, nosql, cloud, development',
    install_requires=requires
)
