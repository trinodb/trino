Trino Plugin
============

This is a [Trino](http://trino.io/) plugin.

# Usage

Download one of the ZIP packages, unzip it and copy the `${artifactId}-0.1` directory to the plugin directory on every node in your Trino cluster.
Create a `${connectorName}.properties` file in your Trino catalog directory and set all the required properties.

```
connector.name=${connectorName}
```

After reloading Trino, you should be able to connect to the `${connectorName}` catalog.

# Build

Run all the unit test classes.
```
mvn test
```

Creates a deployable jar file
```
mvn clean compile package
```

Copy jar files in target directory to use git connector in your Trino cluster.
```
cp -p target/*.jar ${PLUGIN_DIRECTORY}/${connectorName}/
```
