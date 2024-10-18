# Clients

A [client](trino-concept-client) is used to send queries to Trino and receive
results, or otherwise interact with Trino and the connected data sources.

Some clients, such as the [command line interface](/client/cli), can provide a
user interface directly. Clients like the [JDBC driver](/client/jdbc), provide a
mechanism for other applications, including your own custom applications, to
connect to Trino.

The following clients are available as part of every Trino release:

```{toctree}
:maxdepth: 1

client/cli
client/jdbc
```

The Trino project maintains the following other client libraries:

* [trino-go-client](https://github.com/trinodb/trino-go-client)
* [trino-js-client](https://github.com/trinodb/trino-js-client)
* [trino-python-client](https://github.com/trinodb/trino-python-client)

In addition, other communities and vendors provide [numerous other client
libraries, drivers, and applications](https://trino.io/ecosystem/client)
