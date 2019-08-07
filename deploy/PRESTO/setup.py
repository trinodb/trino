#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

test_requirements = [
    'tox==1.9.2',
    'nose==1.3.7',
    'mock==1.0.1',
]


setup(
    name='ambari-presto',
    version='1.2',
    description='This project contains the integration code for integrating \
Presto as a service in Ambari.',
    long_description=readme + '\n\n' + history,
    author='Teradata Corporation',
    author_email='anton.petrov@teradata.com',
    url='https://github.com/TeradataCenterForHadoop/ambari-presto-service',
    packages=['package.scripts'],
    include_package_data=True,
    license='APLv2',
    zip_safe=False,
    keywords=['presto', 'ambari', 'hadoop'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: APLv2 License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    test_suite='tests',
    tests_require=test_requirements,
)
