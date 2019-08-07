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

.PHONY: clean-pyc clean-build clean-test test clean help

help:
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-test - remove test artifacts"
	@echo "test - run all unit tests located in the tests directory"
	@echo "clean-docs - remove documentation artifacts"
	@echo "clean - remove all files and folders that are not checked into the repo"
	@echo "dist - package and build integration code in a tar.gz"
	@echo "docs - generate Sphinx HTML documentation"
	@echo "open-docs - open the root document (index.html) using xdg-open"
	@echo "help - display this help menu"

clean: clean-pyc clean-build clean-test clean-docs

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr ambari_presto.egg-info/
	rm -fr .eggs/

clean-test:
	rm -rf .tox/

clean-docs:
	rm -rf docs/_build

test: clean-test
	tox -- -s tests

dist: clean-build clean-pyc
ifdef VERSION
	sed -i 's%<version>.*</version>%<version>$(VERSION)</version>%' metainfo.xml
endif
	python setup.py sdist
	ls -l dist

docs: clean-docs
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

open-docs:
	xdg-open docs/_build/html/index.html
