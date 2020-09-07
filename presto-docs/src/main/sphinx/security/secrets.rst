=======
Secrets
=======

Presto manages configuration details in static properties files. This
configuration needs to include values such as usernames, passwords and other
strings, that are often required to be kept secret. Only a few select
administrators or the provisioning system has access to the actual value.

The secrets support in Presto allows you to use environment variables as values
for any configuration property. All properties files used by Presto, including
``config.properties`` and all catalog properties files are supported. When
loading the properties, Presto simply replaces the reference to the environment
variable with the value of the environment variable.

Environment variables are the most widely supported means of setting and
retrieving values. Most provisioning and configuration management systems
include support for setting environment variables. This includes systems such as
Ansible, often used for virtual machines, and Kubernetes for container usage.
Setting an environment variable can be done manually on the command line as
well.

.. code-block:: none

    export DB_PASSWORD=my-super-secret-pwd

To use this variable in the properties file, you reference it with the syntax
``${ENV:VARIABLE}``. For example, if you want to use the password in a catalog
properties file like ``etc/catalog/db.properties``, add the following line:

.. code-block:: properties

    connection-password=${ENV:DB_PASSWORD}

With this setup in place the secret is only managed by the provisioning system
or the administrators handling the machines. No secret is stored in the Presto
configuration files on the filesystem and wherever they are managed.
