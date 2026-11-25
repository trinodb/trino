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

# The sphinx-immaterial theme derives the page table of contents from the
# global table of contents. The toctrees in the docs use the titlesonly option
# to keep the left navigation limited to page titles, which leaves the global
# table of contents without any page sections, so the theme renders no page
# navigation. Build the page table of contents from the page itself instead,
# like the theme already does for the root document.

from sphinx.environment.adapters.toctree import TocTree
from sphinx_immaterial.nav_adapt import _get_mkdocs_toc


def set_page_toc(app, pagename, templatename, context, doctree):
    page = context.get('page')
    if page is None or doctree is None:
        return

    toc_node = TocTree(app.env).get_toc_for(pagename, app.builder)
    toc = _get_mkdocs_toc(toc_node, app.builder)
    # The single top level heading is the page title
    if len(toc) == 1:
        toc = toc[0].children

    page['toc'] = toc
    page['integrated_local_toc'] = toc
    page['canonical_url'] = context.get('pageurl')


def setup(app):
    # Run after the theme populates the page context
    app.connect('html-page-context', set_page_toc, priority=900)
    return {
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
