==============
Group provider
==============

Trino can map user names onto groups for easier access control management.
This mapping is performed by a ``GroupProvider`` implementation.

Implementation
--------------

``GroupProviderFactory`` is responsible for creating a ``GroupProvider`` instance.
It also defines the name of the group provider as used in the configuration file.

``GroupProvider`` contains a one method, ``getGroups(String user)``
which returns a ``Set<String>`` of group names.
This set of group names becomes part of the ``Identity`` and ``ConnectorIdentity``
objects representing the user, and can then be used by :doc:`system-access-control`.

The implementation of ``GroupProvider`` and its corresponding ``GroupProviderFactory``
must be wrapped as a Trino plugin and installed on the cluster.

Configuration
-------------

After a plugin that implements ``GroupProviderFactory`` has been installed on the coordinator,
it is configured using an ``etc/group-provider.properties`` file.
All of the properties other than ``group-provider.name`` are specific to
the ``GroupProviderFactory`` implementation.

The ``group-provider.name`` property is used by Trino to find a registered
``GroupProviderFactory`` based on the name returned by ``GroupProviderFactory.getName()``.
The remaining properties are passed as a map to
``GroupProviderFactory.create(Map<String, String>)``.

Example configuration file:

.. code-block:: text

    group-provider.name=custom-group-provider
    custom-property1=custom-value1
    custom-property2=custom-value2

With that file in place, Trino will attempt user group name resolution,
and will be able to use the group names while evaluating access control rules.
