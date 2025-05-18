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

# -- General configuration -----------------------------------------------------

needs_sphinx = '3.0'

extensions = [
    'myst_parser',
    'backquote',
    'download',
    'issue',
    'sphinx_copybutton',
    'redirects',
    'sphinxcontrib.jquery',
    'sphinx_immaterial'
]

redirects_file = 'redirects.txt'

templates_path = ['templates']

source_suffix = '.md'

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
html_baseurl = 'https://trino.io/docs/current/'

html_permalinks = True
html_permalinks_icon = '#'
html_show_copyright = False
html_show_sphinx = False

html_sidebars = {
    "**": ['logo-text.html', 'globaltoc.html', 'localtoc.html', 'searchbox.html']
}

html_theme_options = {
    'site_url': html_baseurl,
    'toc_title': 'Contents',
    'features': [
        'content.action.edit',
        'content.action.view',
        'content.code.copy',
        'content.tabs.link',
        'content.tooltips'
        'navigation.expand',
        'navigation.footer',
        'navigation.sections',
        'navigation.top',
        'navigation.tracking',
        'search.share',
        'search.suggest',
        'toc.follow',
    ],
    'icon': {
        'repo': 'fontawesome/brands/github-alt',
        'edit': 'material/file-edit-outline',
        'view': 'material/file-eye-outline'
    },
    'palette': [
        {
            'media': '(prefers-color-scheme)',
            'scheme': 'default',
            'toggle': {
                'icon': 'material/brightness-auto',
                'name': 'Switch to light mode',
            }
        },
        {
            'media': '(prefers-color-scheme: light)',
            'scheme': 'default',
            'toggle': {
                'icon': 'material/weather-sunny',
                'name': 'Switch to dark mode',
            }
        },
        {
            'media': '(prefers-color-scheme: dark)',
            'scheme': 'slate',
            'toggle': {
                'icon': 'material/weather-night',
                'name': 'Switch to system preference',
            }
        },
    ],
    'repo_url': 'https://github.com/trinodb/trino',
    'repo_name': 'Trino',
    'edit_uri': 'blob/master/docs/src/main/sphinx'
}

html_css_files = [
    'trino.css',
]

suppress_warnings = [
    'config.cache'
]

object_description_options = [
    ("c:.*", dict(toc_icon_class=None, toc_icon_text=None)),
    ("cpp:.*", dict(toc_icon_class=None, toc_icon_text=None)),
    ("py:.*", dict(toc_icon_class=None, toc_icon_text=None)),
    ("json:.*", dict(toc_icon_class=None, toc_icon_text=None)),
    ("std:.*", dict(toc_icon_class=None, toc_icon_text=None))
]
