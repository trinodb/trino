# Python user-defined functions

A Python user-defined function is a [user-defined function](/udf) that uses the
[Python programming language and statements](python-udf-lang) for the definition
of the function.

## Python UDF declaration

Declare a Python UDF as [inline](udf-inline) or [catalog UDF](udf-catalog) with
the following steps:

* Use the [](/udf/function) keyword to declare the UDF name and parameters.
* Add the `RETURNS` declaration to specify the data type of the result.
* Set the `LANGUAGE` to `PYTHON`.
* Declare the name of the Python function to call with the `handler` property in
  the `WITH` block.
* Use `$$` to enclose the Python code after the `AS` keyword.
* Add the function from the handler property and ensure it returns the declared
  data type.
* Expand your Python code section to implement the function using the available
  [Python language](python-udf-lang).

The following snippet shows pseudo-code:

```text
FUNCTION python_udf_name(input_parameter data_type)
  RETURNS result_data_type
  LANGUAGE PYTHON
  WITH (handler = 'python_function')
  AS $$
  ...
  def python_function(input):
      return ...
  ...
  $$
```

A minimal example declares the UDF `doubleup` that returns the input integer
value `x` multiplied by two. The example shows declaration as [](udf-inline) and
invocation with the value `21` to yield the result `42`.

Set the language to `PYTHON` to override the default `SQL` for [](/udf/sql).
The Python code is enclosed with `$$` and must use valid formatting.

```text
WITH
  FUNCTION doubleup(x integer)
    RETURNS integer
    LANGUAGE PYTHON
    WITH (handler = 'twice')
    AS $$
    def twice(a):
        return a * 2
    $$
SELECT doubleup(21);
-- 42
```

The same UDF can also be declared as [](udf-catalog).

Refer to the [](/udf/python/examples) for more complex use cases and examples.

```{toctree}
:titlesonly: true
:hidden:

/udf/python/examples
```

(python-udf-lang)=
## Python language details

The Trino Python UDF integrations uses Python 3.13.0 in a sandboxed environment.
Python code runs within a WebAssembly (WASM) runtime within the Java virtual
machine running Trino.

Python language rules including indents must be observed.

Python UDFs therefore only have access to the Python language and core libraries
included in the sandboxed runtime. Access to external resources with network or
file system operations is not supported. Usage of other Python libraries as well
as command line tools or package managers is not supported.

The following libraries are explicitly removed from the runtime and therefore
not available within a Python UDF:

* `bdb`
* `concurrent`
* `curses`
* `ensurepip`
* `doctest`
* `idlelib`
* `multiprocessing`
* `pdb`
* `pydoc`
* `socketserver`
* `sqlite3`
* `ssl`
* `subprocess`
* `tkinter`
* `turtle`
* `unittest`
* `venv`
* `webbrowser`
* `wsgiref`
* `xmlrpc`

The following libraries are explicitly added to the runtime and therefore
available within a Python UDF:

* `attrs`
* `bleach`
* `charset-normalizer`
* `defusedxml`
* `idna`
* `jmespath`
* `jsonschema`
* `pyasn1`
* `pyparsing`
* `python-dateutil`
* `rsa`
* `tomli`
* `ua-parser`

## Type mapping

The following table shows supported Trino types and their corresponding Python
types for input and output values of a Python UDF:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Trino type
  - Python type
* - `ROW`
  - `tuple`
* - `ARRAY`
  - `list`
* - `MAP`
  - `dict`
* - `BOOLEAN`
  - `bool`
* - `TINYINT`
  - `int`
* - `SMALLINT`
  - `int`
* - `INTEGER`
  - `int`
* - `BIGINT`
  - `int`
* - `REAL`
  - `float`
* - `DOUBLE`
  - `float`
* - `DECIMAL`
  - `decimal.Decimal`
* - `VARCHAR`
  - `str`
* - `VARBINARY`
  - `bytes`
* - `DATE`
  - `datetime.date`
* - `TIME`
  - `datetime.time`
* - `TIME WITH TIME ZONE`
  - `datetime.time` with `datetime.tzinfo`
* - `TIMESTAMP`
  - `datetime.datetime`
* - `TIMESTAMP WITH TIME ZONE`
  - `datetime.datetime` with `datetime.tzinfo`
* - `INTERVAL YEAR TO MONTH`
  - `int` as the number of months
* - `INTERVAL DAY TO SECOND`
  - `datetime.timedelta`
* - `JSON`
  - `str`
* - `UUID`
  - `uuid.UUID`
* - `IPADDRESS`
  - `ipaddress.IPv4Address` or `ipaddress.IPv6Address`

:::

### Time and timestamp

Python `datetime` and `time` objects only support microsecond precision.
Trino argument values with greater precision are rounded when converted to
Python values, and Python return values are rounded if the Trino return type
has less than microsecond precision.

### Timestamp with time zone

Only fixed offset time zones are supported. Timestamps with political time zones
have the zone converted to the zone's offset for the timestamp's instant.
