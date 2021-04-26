#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

requirements = [
    'click',
    'pandas',
    'pyspark',
    'findspark',
    'isodate',
    'requests'
]

test_requirements = [
    'unittest',
    'json'
]

setup(
    name='recipes_etl',
    version='0.0.1',
    description="",
    author="Kamil Gusowski",
    author_email='kamil.gusowski@gmail.com',
    url='https://github.com/gkns1/recipes_etl',
    packages=[
        'recipes_etl',
    ],
    package_dir={'recipes_etl': 'recipes_etl'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='recipes etl spark impala',
    test_suite='tests',
    tests_require=test_requirements
)