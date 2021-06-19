============
SPI overview
============

When you implement a new Trino plugin, you implement interfaces and
override methods defined by the SPI.

Plugins can provide additional :doc:`connectors`, :doc:`types`,
:doc:`functions` and :doc:`system-access-control`.
In particular, connectors are the source of all data for queries in
Trino: they back each catalog available to Trino.

Code
----

The SPI source can be found in the ``trino-spi`` directory in the
root of the Trino source tree.

Plugin metadata
---------------

Each plugin identifies an entry point: an implementation of the
``Plugin`` interface. This class name is provided to Trino via
the standard Java ``ServiceLoader`` interface: the classpath contains
a resource file named ``io.trino.spi.Plugin`` in the
``META-INF/services`` directory. The content of this file is a
single line listing the name of the plugin class:

.. code-block:: text

    com.example.plugin.ExamplePlugin

For a built-in plugin that is included in the Trino source code,
this resource file is created whenever the ``pom.xml`` file of a plugin
contains the following line:

.. code-block:: xml

    <packaging>trino-plugin</packaging>

Plugin
------

The ``Plugin`` interface is a good starting place for developers looking
to understand the Trino SPI. It contains access methods to retrieve
various classes that a Plugin can provide. For example, the ``getConnectorFactories()``
method is a top-level function that Trino calls to retrieve a ``ConnectorFactory`` when Trino
is ready to create an instance of a connector to back a catalog. There are similar
methods for ``Type``, ``ParametricType``, ``Function``, ``SystemAccessControl``, and
``EventListenerFactory`` objects.

Building plugins via Maven
--------------------------

Plugins depend on the SPI from Trino:

.. code-block:: xml

    <dependency>
        <groupId>io.trino</groupId>
        <artifactId>trino-spi</artifactId>
        <scope>provided</scope>
    </dependency>

The plugin uses the Maven ``provided`` scope because Trino provides
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

For an example ``pom.xml`` file, see the example HTTP connector in the
``trino-example-http`` directory in the root of the Trino source tree.

Deploying a custom plugin
-------------------------

In order to add a custom plugin to a Trino installation, create a directory
for that plugin in the Trino plugin directory and add all the necessary jars
for the plugin to that directory. For example, for a plugin called
``my-functions``, you would create a directory ``my-functions`` in the Trino
plugin directory and add the relevant jars to that directory.

By default, the plugin directory is the ``plugin`` directory relative to the
directory in which Trino is installed, but it is configurable using the
configuration variable ``plugin.dir``. In order for Trino to pick up
the new plugin, you must restart Trino.

Plugins must be installed on all nodes in the Trino cluster (coordinator and workers).
