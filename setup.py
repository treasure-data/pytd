"""Treasure Data Driver for Python

pytd is a Python interface to Treasure Data data management platform.
"""
import ast
import re


with open('pytd/version.py', 'rb') as f:
    version_re = re.compile(r'__version__\s+=\s+(.*)')
    VERSION = str(ast.literal_eval(version_re.search(
        f.read().decode('utf-8')).group(1)))

DISTNAME = 'pytd'
DESCRIPTION = 'Treasure Data Driver for Python'
LONG_DESCRIPTION = __doc__ or ''
AUTHOR = 'Takuya Kitazawa'
AUTHOR_EMAIL = 'kitazawa@treasure-data.com'
MAINTAINER = AUTHOR
MAINTAINER_EMAIL = AUTHOR_EMAIL
LICENSE = 'Apache License 2.0'
URL = 'https://github.com/takuti/pytd'


def setup_package():
    from setuptools import setup, find_packages

    metadata = dict(
        name=DISTNAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        maintainer=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        license=LICENSE,
        url=URL,
        classifiers=['Development Status :: 1 - Planning',
                     'Intended Audience :: Developers',
                     'License :: OSI Approved :: Apache Software License',
                     'Topic :: Database',
                     'Programming Language :: Python',
                     'Programming Language :: Python :: 2',
                     'Programming Language :: Python :: 2.7',
                     'Programming Language :: Python :: 3',
                     'Programming Language :: Python :: 3.4',
                     'Programming Language :: Python :: 3.5',
                     'Programming Language :: Python :: 3.6'],
        packages=find_packages(exclude=['*tests*']),
        install_requires=[
            'presto_python_client @ git+https://github.com/prestodb/presto-python-client.git@0.6.0#egg=presto_python_client',
            'pandas>=0.22.0',
            'six>=1.11.0',
            'td-client>=0.12.0',
            'pytz>=2018.5',
        ],
        extras_require={
            'spark': ['pyspark>=2.4.0', 'pyarrow>=0.11.0'],
        })

    setup(**metadata)


if __name__ == '__main__':
    setup_package()
