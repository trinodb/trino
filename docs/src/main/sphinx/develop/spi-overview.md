# SPI overview

Trino uses a plugin architecture to extend its capabilities and integrate with
various data sources and other systems. Plugins must implement the interfaces
and override methods defined by the Service Provider Interface (SPI).

General user information about plugins is available in the [Plugin
documentation](/installation/plugins), and specific details are documented in
dedicated pages about each plugin.

This section details plugin development including specific pages for
capabilities of different plugins. A custom plugin enables you to add further
features to Trino, such as a connector for another data source.

## Code

The SPI source can be found in the `core/trino-spi` directory in the Trino
source tree.

## Plugin metadata

Each plugin identifies an entry point: an implementation of the
`Plugin` interface. This class name is provided to Trino via
the standard Java `ServiceLoader` interface: the classpath contains
a resource file named `io.trino.spi.Plugin` in the
`META-INF/services` directory. The content of this file is a
single line listing the name of the plugin class:

```text
com.example.plugin.ExamplePlugin
```

For a built-in plugin that is included in the Trino source code,
this resource file is created whenever the `pom.xml` file of a plugin
contains the following line:

```xml
<packaging>trino-plugin</packaging>
```

## Plugin

The `Plugin` interface is a good starting place for developers looking
to understand the Trino SPI. It contains access methods to retrieve
various classes that a Plugin can provide. For example, the `getConnectorFactories()`
method is a top-level function that Trino calls to retrieve a `ConnectorFactory` when Trino
is ready to create an instance of a connector to back a catalog. There are similar
methods for `Type`, `ParametricType`, `Function`, `SystemAccessControl`, and
`EventListenerFactory` objects.

## Building plugins via Maven

Plugins depend on the SPI from Trino:

```xml
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-spi</artifactId>
    <scope>provided</scope>
</dependency>
```

The plugin uses the Maven `provided` scope because Trino provides
the classes from the SPI at runtime and thus the plugin should not
include them in the plugin assembly.

There are a few other dependencies that are provided by Trino,
including Slice and Jackson annotations. In particular, Jackson is
used for serializing connector handles and thus plugins must use the
annotations version provided by Trino.

All other dependencies are based on what the plugin needs for its
own implementation. Plugins are loaded in a separate class loader
to provide isolation and to allow plugins to use a different version
of a library that Trino uses internally.

For an example `pom.xml` file, see the example HTTP connector in the
`plugin/trino-example-http` directory in the Trino source tree.

## Deploying a custom plugin

Trino plugins must use the `trino-plugin` Maven packaging type provided by the
[trino-maven-plugin](https://github.com/trinodb/trino-maven-plugin). Building a
plugin generates the required service descriptor and invokes
[Provisio](https://github.com/jvanzyl/provisio) to create a ZIP file in the
`target` directory. The file contains the plugin JAR and all its dependencies as
JAR files, and is suitable for [plugin installation](plugins-installation).

(spi-compatibility)=
## Compatibility

Successful [download](plugins-download), [installation](plugins-installation),
and use of a plugin depends on compatibility of the plugin with the target Trino
cluster. Full compatibility is only guaranteed when using the same Trino version
used for the plugin build and the deployment, and therefore using the same
version is recommended.

For example, a Trino plugin compiled for Trino 470 may not work with older or
newer versions of Trino such as Trino 430 or Trino 490. This is specifically
important when installing plugins from other projects, vendors, or your custom
development. 

Trino plugins implement the SPI, which may change with every Trino release.
There are no runtime checks for SPI compatibility by default, and it is up to
the plugin author to verify compatibility using runtime testing. 

If the source code of a plugin is available, you can confirm the Trino version
by inspecting the `pom.xml`. A plugin must declare a dependency to the SPI, and
therefore compatibility with the Trino release specified in the `version` tag:

```xml
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-spi</artifactId>
    <version>470</version>
    <scope>provided</scope>
</dependency>
```

A good practice for plugins is to use a property for the version value, which is
then declared elsewhere in the `pom.xml`:

```xml
...
<dep.trino.version>470</dep.trino.version>
...
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-spi</artifactId>
    <version>${dep.trino.version}</version>
    <scope>provided</scope>
</dependency>
```
