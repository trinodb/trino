===============================
XML of functions and operators
===============================

These functions and operators are used for parsing XML data using XPath expressions.
They are migrated from `Hive xpath and related functions<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+XPathUDF>`_

.. function:: xpath(xml_string, xpath_expression_string) -> array
    :noindex:

    Returns array of strings  If the expression results in a non-text value (e.g., another xml node)
    the function will return an empty array. There are 2 primary uses for this function:
    to get a list of node text values or to get a list of attribute values.::

        SELECT xpath('<a><b>b1</b><b>b2</b></a>','a/*'); -- []
        SELECT xpath('<a><b>b1</b><b>b2</b></a>','a/*/text()'); -- [b1, b2]

        SELECT xpath('<a><b id="foo">b1</b><b id="bar">b2</b></a>','//@id'); -- [foo, bar]
        SELECT xpath ('<a><b class="bb">b1</b><b>b2</b><b>b3</b><c class="bb">c1</c><c>c2</c></a>', 'a/*[@class="bb"]/text()'); -- [b1, c1]

.. function:: xpath_string(xml_string, xpath_expression_string) -> varchar
    :noindex::

    Returns the text of the first matching node.::

        SELECT xpath_string('<a><b>bb</b><c>cc</c></a>', 'a/b'); -- bb
        SELECT xpath_string('<a><b>bb</b><c>cc</c></a>', 'a'); -- bbcc
        SELECT xpath_string('<a><b>bb</b><c>cc</c></a>', 'a/d'); --
        SELECT xpath_string('<a><b>b1</b><b>b2</b></a>', '//b'); -- b1
        SELECT xpath_string ('<a><b>b1</b><b>b2</b></a>', 'a/b[2]'); -- b2
        SELECT xpath_string ('<a><b>b1</b><b id="b_2">b2</b></a>', 'a/b[@id="b_2"]'); -- b2

.. function:: xpath_boolean(xml_string, xpath_expression_string) -> boolean
    :noindex::

    Returns true if the XPath expression evaluates to true, or if a matching node is found.::

        SELECT xpath_boolean('<a><b>b</b></a>', 'a/b'); -- true
        SELECT xpath_boolean('<a><b>b</b></a>', 'a/c'); -- false
        SELECT xpath_boolean ('<a><b>b</b></a>', 'a/b = "b"'); -- true

.. function:: xpath_long(xml_string, xpath_expression_string) -> boolean
    :noindex::

     Returns an integer numeric value, or the value zero if no match is found, or a match is found but the value
     is non-numeric. Mathematical operations are supported. In cases where the value overflows the return type,
     then the maximum value for the type is returned..::

        SELECT xpath_long('<a>b</a>', 'a = 10'); -- 0
        SELECT xpath_long('<a>this is not a number</a>', 'a'); -- 0
        SELECT xpath_long('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/*)'); -- 15
        SELECT xpath_long('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/b)'); -- 7

.. function:: xpath_double(xml_string, xpath_expression_string) -> boolean
    :noindex::

     Returns a float numeric value, or the value zero if no match is found, or the value of NaN if a match is found but the value
     is non-numeric. Mathematical operations are supported. In cases where the value overflows the return type,
     then the maximum value for the type is returned..::

        SELECT xpath_double ('<a>b</a>', 'a = 10'); -- 0.0
        SELECT xpath_double ('<a>this is not a number</a>', 'a'); -- NaN
        SELECT xpath_double ('<a><b>2000000000</b><c>40000000000</c></a>', 'a/b * a/c'); -- 8.0E19
