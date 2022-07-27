Trino Plugin
============

This is a [Trino](http://trino.io/) plugin that provides a connector.

## Usage

Download one of the ZIP packages, unzip it and copy the `trino-example-plugin-0.1` directory to the plugin directory on every node in your Trino cluster.
Create a `example.properties` file in your Trino catalog directory and set all the required properties.

```
connector.name=example
```

After reloading Trino, you should be able to connect to the `example` catalog.

## Build

Run all the unit tests:
```bash
mvn test
```

Creates a deployable zip file:
```bash
mvn clean package
```

Unzip the archive from the target directory to use the connector in your Trino cluster.
```bash
unzip target/*.zip -d ${PLUGIN_DIRECTORY}/
mv ${PLUGIN_DIRECTORY}/trino-example-plugin-* ${PLUGIN_DIRECTORY}/trino-example-plugin 
```

## Debug

To test and debug the connector locally, run the `ExampleQueryRunner` class located in tests:
```bash
mvn test-compile exec:java -Dexec.mainClass="com.example.ExampleQueryRunner" -Dexec.classpathScope=test
```

And then run the Trino CLI using `trino --server localhost:8080 --no-progress` and query it:
```
trino> show catalogs;
 Catalog
---------
 example
 system
(2 rows)

trino> show tables from example.default;
   Table
------------
 single_row
(1 row)

trino> select * from example.default.single_row;
 id |     type      |  name
----+---------------+---------
 x  | default-value | my-name
(1 row)
```
