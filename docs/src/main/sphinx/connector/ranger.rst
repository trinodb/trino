=======================
Apache Ranger connector
=======================

.. raw:: html

  <img src="../_static/img/apache_ranger.png" class="connector-logo">

https://ranger.apache.org/

Apache Ranger is a framework to enable comprehensive data security across many platforms. Trino is one of these platforms.

Requirements
------------

To connect to Apache Ranger you need:

* Apache Ranger installed, up and running.
* Network access from the Trino coordinator to the Apache Ranger HTTPS port.

Configuration
-------------

The connector downloads the JSON policies from the configured Trino service in the Ranger UI editor.

Example access-control-ranger.properties file:

.. code-block:: text

    access-control.name=ranger
    ranger.use_ugi=true
    ranger.service_name=trino-dev-system
    ranger.hadoop_config=/workspace/testing/trino-server-dev/etc/trino-ranger-site.xml
    ranger.audit_resource=/workspace/testing/trino-server-dev/etc/trino-ranger-audit.xml
    ranger.security_resource=/workspace/testing/trino-server-dev/etc/trino-ranger-security.xml
    ranger.policy_manager_ssl_resource=/workspace/testing/trino-server-dev/etc/trino-ranger-policymgr-ssl.xml

In the Trino config.properties file set the below so it properly points to the above configuration file

.. code-block:: text

    ...
    access-control.config-files=/etc/trino/access-control-ranger.properties


use_ugi (aka UserGroupInformation): Tells the plugin to map users and groups together.
Its much simpler to manage groups of users than individual users. Setting to true
is a requirement if you are going to use corporate AD/LDAP to manage access controls.

service_name as defined in the Ranger UI

hadoop_config (aka trino-ranger-site.xml): is the Ranger site file required to connect
to corporate AD/LDAP systems. When the user logs into trino, this is the file used to
connect to the AD/LDAP system and get a list of the users groups.

audit_resource: Ranger can be configured to send reports to various systems.
At the moment this file is required for legacy reasons and should be cleaned up
in the figure. all DEFAULT settings is recommented. If you want auditing use the
trino-http-event-listener and post those events to the HTTP service of your choice.
We recommend using it to post to Kafla but its very flexible.

security_resource (aka trino-ranger-security.xml): Configures the connectivity
between the trino-ranger plugin and the Apache Ranger running service.

policy_manager_ssl_resource (aka trino-ranger-policymgr-ssl.xml): Used to setup
up 2 way SSL client/server validation
