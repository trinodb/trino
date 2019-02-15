========================
Ranger Plugin Tutorial
========================

.. contents::
    :local:
    :backlinks: none
    :depth: 2

Introduction
============

The Ranger plugin for presto allows security policies defined in apache ranger to be used in the presto execution engine. As of now only ranger access policies are supported.

Installation
============

This tutorial assumes familiarity with Presto and a working local Presto
installation (see :doc:`/installation/deployment`). Also it assumes ranger is already set up.
.. note::

    This tutorial was tested with Apache Ranger 0.7.1.
    It should work with any 0.7.x version of Apache Ranger.


Step 1: Set Ranger configurations
----------------------------

You need to change the ranger-hive-security.xml with the appropriate ranger url. Either you can copy the ranger-hive-security.xml from your setup or you can checkout src/test/resources for a sample.
Change the property <name>ranger.plugin.hive.policy.rest.url</name> with the appropriate urls.
Change the property <name>ranger.service.store.rest.url</name> with the appropriate url.
Change the property <name>ranger.plugin.hive.service.name</name> with the name hive service is registered in the ranger service. Its visible in the ranger UI.

You can tweak the ranger audit xml later.


Step 2: Set access control properties
-----------------

In your Presto installation, add a access control properties file
``etc/access-control.properties`` for the Ranger Plugin.

.. code-block:: none

    access-control.name=ranger-access-control
    ranger-service-types=hive
    ranger-app-ids=hive
    ranger-security-config-hive=<Path of ranger-hive-security.xml>
    ranger-audit-config-hive=<Path of ranger-hive-audit.xml>
    ranger.username=***
    ranger.password=****


ranger-service-types is used as the catalog for which ranger plugin is enabled. Catalogs which are not part of the service-types configs has allow-all access.
ranger-app-id is the identifier which is used by ranger to identify this plugin. You can see this information as part of the ranger plugin audit tab in the ranger ui.


Now start Presto:

.. code-block:: none

    $ bin/launcher start


Start the :doc:`Presto CLI </installation/cli>`:

.. code-block:: none

    $ ./presto --catalog hive

List the tables to verify that things are working:

.. code-block:: none

    presto> SHOW SCHEMAS;


