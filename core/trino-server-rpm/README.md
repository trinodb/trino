# Trino RPM

## RPM Package Build And Usage

You can build an RPM package for Trino server and install Trino using the RPM. Thus, the installation is easier to manage on RPM-based systems.

The RPM builds by default in Maven, and can be found under the directory `core/trino-server-rpm/target`.

To install Trino using an RPM, run:

    rpm -i trino-server-rpm-<version>-noarch.rpm

This will install Trino in single node mode, where both coordinator and workers are co-located on localhost.
This will deploy the necessary default configurations along with a service script to control the Trino server process.

Uninstalling the RPM is like uninstalling any other RPM, just run:

    rpm -e trino-server-rpm-<version>

Note: During uninstall, any Trino related files deployed will be deleted except for the Trino logs directory `/var/log/trino`.

## Control Scripts

The Trino RPM will also deploy service scripts to control the Trino server process.
The script is configured with `chkconfig`,  so that the service can be started automatically on OS boot.
After installing Trino from the RPM, you can run:

    service trino [start|stop|restart|status]

## Installation directory structure

We use the following directory structure to deploy various Trino artifacts.

* `/usr/lib/trino/lib/`: Various libraries needed to run the product. Plugins go in a `plugin` subdirectory.
* `/etc/trino`: General Trino configuration files like `node.properties`, `jvm.config`, `config.properties`. Connector configs go in a `catalog` subdirectory
* `/etc/trino/env.sh`: Java installation path used by Trino
* `/var/log/trino`: Log files
* `/var/lib/trino/data`: Data directory
* `/usr/shared/doc/trino`: Docs
* `/etc/rc.d/init.d/trino`: Control script

The node.properties file requires the following two additional properties since our directory structure is different from what standard Trino expects.

    catalog.config-dir=/etc/trino/catalog
