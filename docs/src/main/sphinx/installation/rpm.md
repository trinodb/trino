# RPM package

Users can install Trino using the RPM Package Manager (RPM) on some Linux
distributions that support RPM.

The RPM archive contains the application, all plugins, the necessary default
configuration files, default setups, and integration with the operating system
to start as a service.

:::{warning}
It is recommended to deploy Trino with the {doc}`Helm chart <kubernetes>` on
Kubernetes or manually with the {doc}`Docker containers <containers>` or the
{doc}`tar archive <deployment>`. While the RPM is available for use, it is
discouraged in favor of the tarball or Docker containers.
:::

## Installing Trino

Download the Trino server RPM package {maven_download}`server-rpm`. Use the
`rpm` command to install the package:

```text
rpm -i trino-server-rpm-*.rpm --nodeps
```

Installing the {ref}`required Java and Python setup <requirements>` must be
managed separately.

## Service script

The RPM installation deploys a service script configured with `systemctl` so
that the service can be started automatically on operating system boot. After
installation, you can manage the Trino server with the `service` command:

```text
service trino [start|stop|restart|status]
```

```{eval-rst}
.. list-table:: ``service`` commands
  :widths: 15, 85
  :header-rows: 1

  * - Command
    - Action
  * - ``start``
    - Starts the server as a daemon and returns its process ID.
  * - ``stop``
    - Shuts down a server started with either ``start`` or ``run``. Sends the
      SIGTERM signal.
  * - ``restart``
    - Stops and then starts a running server, or starts a stopped server,
      assigning a new process ID.
  * - ``status``
    - Prints a status line, either *Stopped pid* or *Running as pid*.
```

## Installation directory structure

The RPM installation places Trino files in accordance with the Linux Filesystem
Hierarchy Standard using the following directory structure:

- `/usr/lib/trino/lib/`: Contains the various libraries needed to run the
  product. Plugins go in a `plugin` subdirectory.
- `/etc/trino`: Contains the general Trino configuration files like
  `node.properties`, `jvm.config`, `config.properties`. Catalog
  configurations go in a `catalog` subdirectory.
- `/etc/trino/env.sh`: Contains the Java installation path used by Trino,
  allows configuring process environment variables, including {doc}`secrets
  </security/secrets>`.
- `/var/log/trino`: Contains the log files.
- `/var/lib/trino/data`: The location of the data directory. Trino stores logs
  and other data here.
- `/etc/rc.d/init.d/trino`: Contains the service scripts for controlling the
  server process, and launcher configuration for file paths.

## Uninstalling

Uninstalling the RPM is like uninstalling any other RPM, just run:

```text
rpm -e trino-server-rpm-<version>
```

Note: During uninstall, all Trino related files are deleted except for
user-created configuration files, copies of the original configuration files
`node.properties.rpmsave` and `env.sh.rpmsave` located in the `/etc/trino`
directory, and the Trino logs directory `/var/log/trino`.
