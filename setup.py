# -*- coding: utf-8 -*-
#

from setuptools import setup
import os


long_description = """
owmeta-core
===========

owmeta-core is a platform for sharing relational data over the internet.
"""


for line in open('owmeta_core/__init__.py'):
    if line.startswith("__version__"):
        version = line.split("=")[1].strip()[1:-1]

package_data_excludes = ['.*', '*.bkp', '~*']


def excludes(base):
    res = []
    for x in package_data_excludes:
        res.append(os.path.join(base, x))
    return res


rdflib_sqlalchemy_dep = 'rdflib-sqlalchemy~=0.4.0.dev0',
setup(
    name='owmeta-core',
    zip_safe=False,
    install_requires=[
        'BTrees',
        'gitpython>=2.1.1',
        'Pint',
        'pow-store-zodb>=0.0.11',
        'rdflib>=4.1.2,!=6.0.0',
        'requests',
        'cachecontrol[filecache]',
        'six~=1.10',
        'tqdm~=4.23',
        'termcolor~=1.1.0',
        'transaction>=1.4.4',
        'wrapt~=1.11.1',
        'zc.lockfile',
        'zodb>=4.1.0',
        'pyyaml',
    ],
    extras_require={
        # SQL source support
        'mysql_source_mysql_connector': [
            rdflib_sqlalchemy_dep,
            'mysql-connector-python'],
        'mysql_source_mysqlclient': [
            rdflib_sqlalchemy_dep,
            'mysqlclient'],
        'postgres_source_psycopg': [
            rdflib_sqlalchemy_dep,
            'psycopg2'],
        'postgres_source_pg8000': [
            rdflib_sqlalchemy_dep,
            'sqlalchemy[postgresql_pg8000]'],
        # Need 1.5.3 for host key file support
        'sftp': 'paramiko>=1.5.3'
    },
    version=version,
    packages=[
        'owmeta_core',
        'owmeta_core.data_trans',
        'owmeta_core.commands',
        'owmeta_core.bundle',
        'owmeta_core.bundle.loaders',
    ],
    author='OpenWorm.org authors and contributors',
    author_email='info@openworm.org',
    description='owmeta-core is a platform for sharing relational data over the internet.',
    long_description=long_description,
    license='MIT',
    url='https://owmeta-core.readthedocs.io/en/latest/',
    entry_points={
        'console_scripts': ['owm = owmeta_core.cli:main'],
        'rdf.plugins.store': [
            'agg = owmeta_core.agg_store:AggregateStore',
            'owmeta_core_bds = owmeta_core.bundle_dependency_store:BundleDependencyStore',
        ],
        'owmeta_core.commands': [
            'bundle.remote.add.sftp = owmeta_core.bundle.loaders.sftp:sftp_remote [sftp]',
            'bundle.remote.update.sftp = owmeta_core.bundle.loaders.sftp:sftp_remote [sftp]',
            'bundle.remote.add.https = owmeta_core.bundle.loaders.http:https_remote',
            'bundle.remote.update.https = owmeta_core.bundle.loaders.http:https_remote',
            'bundle.remote.add.http = owmeta_core.bundle.loaders.http:http_remote',
            'bundle.remote.update.http = owmeta_core.bundle.loaders.http:http_remote',
        ],
        'owmeta_core.loaders': [
            'http_uploader = owmeta_core.bundle.loaders.http:HTTPBundleUploader',
            'http_loader = owmeta_core.bundle.loaders.http:HTTPBundleLoader',
            'file_loader = owmeta_core.bundle.loaders.local:FileBundleLoader',
            'sftp_uploader = owmeta_core.bundle.loaders.sftp:DumbSFTPUploader [sftp]',
        ],
    },
    package_data={'owmeta_core': ['default.conf']},
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering'
    ]
)
