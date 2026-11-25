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

#
# Trino documentation build configuration file
#
# This file is execfile()d with the current directory set to its containing dir.
#

import os
import sys
import xml.dom.minidom

from sphinx.domains.python import PythonDomain

try:
    sys.dont_write_bytecode = True
except:
    pass

sys.path.insert(0, os.path.abspath('ext'))


def child_node(node, name):
    for i in node.childNodes:
        if (i.nodeType == i.ELEMENT_NODE) and (i.tagName == name):
            return i
    return None


def node_text(node):
    return node.childNodes[0].data


def maven_version(pom):
    dom = xml.dom.minidom.parse(pom)
    project = dom.childNodes[0]

    version = child_node(project, 'version')
    if version:
        return node_text(version)

    parent = child_node(project, 'parent')
    version = child_node(parent, 'version')
    return node_text(version)


def get_version():
    version = os.environ.get('TRINO_VERSION', '').strip()
    return version or maven_version('../../../pom.xml')


def globalReplace(app, docname, source):
    result = source[0]
    for key in app.config.global_replacements:
        result = result.replace(key, app.config.global_replacements[key])
    source[0] = result


def setup(app):
   app.add_config_value('global_replacements', {}, True)
   app.connect('source-read', globalReplace)
   # SQL functions are documented with the default Python domain directives,
   # so label them as SQL objects in the search results and link tooltips
   PythonDomain.label = 'SQL'

# -- General configuration -----------------------------------------------------

needs_sphinx = '3.0'

extensions = [
    'myst_parser',
    'backquote',
    'download',
    'issue',
    'redirects',
    'sphinx_immaterial',
    'page_toc',
]

redirects_file = 'redirects.txt'

templates_path = ['templates']

source_suffix = '.rst'

master_doc = 'index'

project = u'Trino'

version = get_version()
release = version

exclude_patterns = ['_build']

highlight_language = 'sql'

default_role = 'backquote'

# Any replace that is inside a code block should be added here
# https://stackoverflow.com/questions/8821511/substitutions-inside-sphinx-code-blocks-arent-replaced

global_replacements = {
    "|trino_version|" : version
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "substitution"
]

myst_substitutions = {
    "breaking": "<a href='../release.html#breaking-changes' title='Breaking change'>⚠️ Breaking change:</a>"
}

# -- Options for HTML output ---------------------------------------------------

html_theme = 'sphinx_immaterial'

html_static_path = ['static']

html_title = '%s %s Documentation' % (project, release)

html_logo = 'images/trino.svg'
html_favicon = 'images/trino.svg'
html_baseurl = 'https://trino.io/docs/current/'

html_permalinks = True
html_permalinks_icon = '#'
html_show_copyright = False
html_show_sphinx = False

# The theme searches from every page and opens the search for the q parameter,
# so redirect links to the old search page there
html_additional_pages = {
    'search': 'search-redirect.html',
}

html_theme_options = {
    'site_url': html_baseurl,
    'repo_url': 'https://github.com/trinodb/trino',
    'repo_name': 'Trino',
    'icon': {
        'repo': 'fontawesome/brands/github',
    },
    # Bundled fonts are loaded from trino.css instead of being downloaded from Google Fonts
    'font': False,
    'globaltoc_collapse': True,
    'toc_title': 'Contents',
    'features': [
        'content.code.copy',
        'navigation.footer',
        'toc.follow',
    ],
}

html_css_files = [
    'trino.css',
]

suppress_warnings = [
    'config.cache'
]

# The theme marks functions and their parameters in the navigation with icons
# that were not part of the documentation look before
object_description_options = [
    ('.*', dict(toc_icon_class=None, toc_icon_text=None)),
]
