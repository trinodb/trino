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

# Sphinx directive that embeds an interactive, runnable SQL example using the
# trysql.io service (https://trysql.io). Usage in Myst Markdown:
#
#     ```{try-sql}
#     SELECT 5 BETWEEN SYMMETRIC 10 AND 1 AS symmetric,
#            5 BETWEEN ASYMMETRIC 10 AND 1 AS asymmetric
#     ```
#
# The Trino version is taken from the documentation build version by default,
# so examples track the version being documented. Override it with the
# ``:version:`` option, and the embed height with ``:height:``.
#
# For examples that need data, put setup statements (CREATE TABLE, INSERT, ...)
# before a line containing only ``---``; trysql.io runs them before the query:
#
#     ```{try-sql}
#     CREATE TABLE memory.default.t (id integer);
#     INSERT INTO memory.default.t VALUES 1, 2, 3;
#     ---
#     SELECT sum(id) FROM memory.default.t
#     ```

from html import escape

# noinspection PyUnresolvedReferences
from docutils import nodes
# noinspection PyUnresolvedReferences
from docutils.parsers.rst import directives
# noinspection PyUnresolvedReferences
from sphinx.util.docutils import SphinxDirective

EMBED_SCRIPT = '<script src="https://trysql.io/embed.js" async></script>'

# A line containing only this token separates an optional setup section (DDL/DML
# run before the query) from the query itself in a try-sql directive body.
SETUP_SEPARATOR = '---'


def split_setup_query(content):
    # content is the directive's source lines. Everything before a line that is
    # exactly SETUP_SEPARATOR is the setup section; the rest is the query. With
    # no separator, the whole body is the query and there is no setup.
    for index, line in enumerate(content):
        if line.strip() == SETUP_SEPARATOR:
            setup = '\n'.join(content[:index]).strip()
            query = '\n'.join(content[index + 1:]).strip()
            return setup, query
    return '', '\n'.join(content).strip()

def resolve_version(config):
    # trysql.io only serves released versions, so a development build version
    # such as "483-SNAPSHOT" must not be sent. Precedence:
    #   1. trysql_version set in conf.py (pin a known-good release like "482")
    #   2. the documentation build version with any "-SNAPSHOT" suffix stripped
    #      (published builds set TRINO_VERSION to a clean release number)
    if config.trysql_version:
        return config.trysql_version
    return config.release.split('-', 1)[0]


class TrySqlDirective(SphinxDirective):
    has_content = True
    option_spec = {
        'version': directives.unchanged,
        'height': directives.unchanged,
    }

    def run(self):
        if not self.content:
            self.error('The try-sql directive requires a SQL query as content')

        version = self.options.get('version') or resolve_version(self.config)
        height = self.options.get('height', '200')
        setup, query = split_setup_query(self.content)

        if not query:
            self.error('The try-sql directive requires a SQL query as content')

        panes = ''
        if setup:
            panes += '<pre data-setup>{}</pre>\n'.format(escape(setup))
        panes += '<pre data-query>{}</pre>\n'.format(escape(query))

        embed = (
            '<try-sql version="{version}" height="{height}" theme="light">\n'
            '{panes}'
            '</try-sql>'
        ).format(version=escape(version), height=escape(height), panes=panes)

        node = nodes.raw('', embed, format='html')
        # Marks the page as using try-sql so the embed script is added to the
        # page header exactly once; see add_embed_script().
        node['trysql'] = True
        return [node]


def add_embed_script(app, pagename, templatename, context, doctree):
    # Inject the trysql.io embed script into the header of each page that uses
    # the try-sql directive, exactly once, and only on those pages.
    if doctree is None:
        return
    if any(node.get('trysql') for node in doctree.findall(nodes.raw)):
        context['metatags'] = context.get('metatags', '') + '\n' + EMBED_SCRIPT


def setup(app):
    app.add_config_value('trysql_version', None, 'env')
    app.add_directive('try-sql', TrySqlDirective)
    app.connect('html-page-context', add_embed_script)

    return {
        'parallel_read_safe': True,
    }
