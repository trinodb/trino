# Forked from https://github.com/jdillard/sphinx-sitemap
#
# Copyright (c) 2013 Michael Dowling <mtdowling@gmail.com>
# Copyright (c) 2017 Jared Dillard <jared.dillard@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

import xml.etree.ElementTree as ET
from xml.dom import minidom


def setup(app):
    """Setup connects events to the sitemap builder"""
    app.connect('html-page-context', add_html_link)
    app.connect('build-finished', create_sitemap)
    app.sitemap_links = []
    app.locales = []

    return {
        'parallel_read_safe': True,
        'parallel_write_safe': False,
    }


def add_html_link(app, pagename, templatename, context, doctree):
    """As each page is built, collect page names for the sitemap"""
    app.sitemap_links.append(pagename + '.html')


def create_sitemap(app, exception):
    """Generates the sitemap.xml from the collected HTML page links"""
    if not app.sitemap_links:
        raise (Exception('sitemap error: No pages generated for sitemap.xml'))

    root = ET.Element('urlset')
    root.set('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9')

    for link in sorted(app.sitemap_links):
        url = ET.SubElement(root, "url")
        ET.SubElement(url, "loc").text = '/' + link

    xml = minidom.parseString(ET.tostring(root)).toprettyxml('  ')

    filename = app.outdir + "/sitemap.xml"
    with open(filename, 'w') as f:
        f.write(xml)
    print('sitemap.xml was generated in %s' % filename)
