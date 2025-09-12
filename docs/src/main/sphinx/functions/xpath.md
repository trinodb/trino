# XPath functions

The XPath functions extract elements from an XML body based on a given path.
The following variations are supported.

:::{function} xpath(xml, path) -> array(varchar)
Returns an array of string where each string element is the value of node that matches the provided path.
:::

:::{function} xpath_node(xml, path) -> varchar
Returns a string whose value is the value of the first node that matches the provided path.
:::

:::{function} xpath_string(xml, path) -> varchar
Returns a string whose value is the result computed based on the provided path.
:::

:::{function} xpath_double(xml, path) -> double
Returns a double whose value is the result computed based on the provided path. Returns 0.0 if no match is found,
or NaN if the match is not numeric.
:::
