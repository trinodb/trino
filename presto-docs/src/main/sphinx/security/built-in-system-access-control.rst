==============================
Built-in System Access Control
==============================

.. toctree::
    :maxdepth: 1
    :hidden:

    File Based <file-system-access-control>

A system access control plugin enforces authorization at a global level,
before any connector level authorization. You can use one of the built-in
plugins in Presto, or provide your own by following the guidelines in
:doc:`/develop/system-access-control`.

Multiple system access control implementations may be configured at once
using the ``access-control.config-files`` configuration property. It should
contain a comma separated list of the access control property files to use
(rather than the default ``etc/access-control.properties``).

Presto offers the following built-in plugins:

================================================== =================================================================
Plugin Name                                        Description
================================================== =================================================================
``default`` (default value)                        All operations are permitted, except for user impersonation.

``allow-all``                                      All operations are permitted.

``read-only``                                      Operations that read data or metadata are permitted, but
                                                   none of the operations that write data or metadata are allowed.

``file``                                           Authorization rules are specified in a config file.
                                                   See :doc:`file-system-access-control`.
================================================== =================================================================

If you want to limit access on a system level in any other way than the ones
listed above, you must implement a custom :doc:`/develop/system-access-control`.

Default System Access Control
===============================

All operations are permitted, except for user impersonation. This plugin is enabled by default.

Allow All System Access Control
===============================

All operations are permitted under this plugin.

Read Only System Access Control
===============================

Under this plugin, you are allowed to execute any operation that reads data or
metadata, such as ``SELECT`` or ``SHOW``. Setting system level or catalog level
session properties is also permitted. However, any operation that writes data or
metadata, such as ``CREATE``, ``INSERT`` or ``DELETE``, is prohibited.
To use this plugin, add an ``etc/access-control.properties``
file with the following contents:

.. code-block:: none

   access-control.name=read-only

File Based System Access Control
================================

This plugin allows you to specify access control rules in a JSON file.
See :doc:`file-system-access-control` for details.
