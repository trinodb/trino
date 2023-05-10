#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Copyright (c) 2017 Stephen Finucane.
# Version 0.1.0 modified April 10, 2023 for Trino use.
# See https://github.com/sphinx-contrib/redirects/pull/6

import os

from sphinx.builders import html as html_builder
from sphinx.builders import dirhtml as dirhtml_builder
from sphinx.util import logging

logger = logging.getLogger(__name__)

TEMPLATE = """<html>
  <head><meta http-equiv="refresh" content="0; url=%s"/></head>
</html>
"""


def generate_redirects(app):

    path = os.path.join(app.srcdir, app.config.redirects_file)
    if not os.path.exists(path):
        logger.info("Could not find redirects file at '%s'", path)
        return

    if isinstance(app.config.source_suffix, dict):
        in_suffixes = list(app.config.source_suffix)
    else:
        in_suffixes = app.config.source_suffix

    if not isinstance(app.builder, html_builder.StandaloneHTMLBuilder):
        logger.warn(
            "The 'sphinxcontib-redirects' plugin is only supported "
            "for the 'html' and 'dirhtml' builder, but you are using '%s'. "
            "Skipping...", type(app.builder)
        )
        return

    from_suffix = to_suffix = '.html'
    if type(app.builder) == dirhtml_builder.DirectoryHTMLBuilder:
        from_suffix = '/index.html'
        to_suffix = '/'

    with open(path) as redirects:
        for line in redirects.readlines():
            from_path, to_path = line.rstrip().split(' ')

            logger.debug("Redirecting '%s' to '%s'", from_path, to_path)

            for in_suffix in in_suffixes:
                if from_path.endswith(in_suffix):
                    from_path = from_path.replace(in_suffix, from_suffix)
                    to_path_prefix = (
                            '..%s'
                            % os.path.sep
                            * (len(from_path.split(os.path.sep)) - 1)
                    )
                    to_path = to_path_prefix + to_path.replace(
                        in_suffix, to_suffix
                    )

            if not to_path:
                raise Exception('failed to find input file!')

            redirected_filename = os.path.join(app.builder.outdir, from_path)
            redirected_directory = os.path.dirname(redirected_filename)
            if not os.path.exists(redirected_directory):
                os.makedirs(redirected_directory)

            with open(redirected_filename, 'w') as f:
                f.write(TEMPLATE % to_path)


def setup(app):
    app.add_config_value('redirects_file', 'redirects', 'env')
    app.connect('builder-inited', generate_redirects)
    return {
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
