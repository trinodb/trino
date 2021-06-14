===========
RPM Package
===========

Users of RedHat Enterprise Linux 7 or 8 and CentOS 7 or 8 can use the RPM package to install SEP. Other versions or distributions are not supported.

The RPM archive includes the application, all plugins, the necessary default configuration files, default setups, and integration with the operating system to start as a service.

The default installation is for single node mode.

- Download the RPM installation package file :maven_download:`server-rpm`
- Use the ``rpm`` or ``yum`` command to install the package:

  .. code-block:: text

    rpm -i trino-server-rpm-*.rpm

Control script
--------------

The RPM installation deploys a service script configured with ``chkconfig`` so that the service can be started automatically on OS boot. After installation, you can manage the SEP server with the ``service`` command:

  .. code-block:: text

    service trino [start|stop|restart|status]

Installation directory structure
--------------------------------

The RPM package places the various files used by SEP in accordance with the Linux Filesystem Hierarchy Standard. This differs from the default ``tar.gz`` installation of SEP, where all folders are in the installation directory. For example, with the ``tar.gz``, configuration files are located by default in the ``etc`` folder of the installation directory. By contrast, the RPM package installation uses ``/etc/trino`` for the same purpose.

The RPM installation places Trino files using the following directory structure:

- ``/usr/lib/trino/lib/`` - Various libraries needed to run the product; plugins go in a ``plugin`` subdirectory
 
- ``/etc/trino`` - General Trino configuration files such as ``config.properties``, ``jvm.config``, and ``node.properties``
 
- ``/etc/trino/catalog`` - Connector configuration files
 
- ``/etc/trino/env.sh`` - Contains the Java installation path used by Trino, allows configuring process environment - variables, including :doc:`secrets </security/secrets>`. 

- ``/var/log/trino`` - Log files
 
- ``/var/lib/trino/data`` - Data directory

- ``/usr/shared/doc/trino`` - Docs

- ``/etc/rc.d/init.d/trino`` - Control script


RPM-specific configuration settings
-----------------------------------

In an RPM installation, the ``node.properties`` file needs two additional properties, because the directory structure is different from the standard established by the ``tar.gz`` installation:

  .. code-block:: text

    node.data-dir=/var/lib/trino/data
    catalog.config-dir=/etc/trino/catalog

Uninstalling
------------

Uninstalling an RPM installation is like uninstalling any other RPM. Run:

  .. code-block:: text

    rpm -e trino-server-rpm-<version>

After uninstalling, all deployed Trino files are deleted except for the logs directory /var/log/trino.