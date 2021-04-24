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
    'unittest'
]

setup(
    name='hellofresh_takehome',
    version='0.0.1',
    description="",
    author="Kamil Gusowski",
    author_email='kamil.gusowski@gmail.com',
    url='https://github.com/gkns1/hellofresh_takehome',
    packages=[
        'hellofresh_takehome',
    ],
    package_dir={'hellofresh_takehome': 'hellofresh_takehome'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='hellofresh takehome',
    test_suite='tests',
    tests_require=test_requirements
)