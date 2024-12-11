# User-defined functions

A user-defined function (UDF) is a custom function authored by a user of Trino
in a client application. UDFs are scalar functions that return a single output
value, similar to [built-in functions](/functions).

[Declare the UDF](udf-declaration) with a `FUNCTION` definition using the
supported statements. A UDF can be declared and used as an [inline
UDF](udf-inline) or declared as a [catalog UDF](udf-catalog) and used
repeatedly.

UDFs are defined and written using the [SQL routine language](/udf/sql). 

More details are available in the following sections:

```{toctree}
:titlesonly: true

udf/introduction
udf/function
udf/sql
```
