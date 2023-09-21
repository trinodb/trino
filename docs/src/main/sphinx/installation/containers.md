# Trino in a Docker container

The Trino project provides the [trinodb/trino](https://hub.docker.com/r/trinodb/trino)
Docker image that includes the Trino server and a default configuration. The
Docker image is published to Docker Hub and can be used with the Docker runtime,
among several others.

## Running the container

To run Trino in Docker, you must have the Docker engine installed on your
machine. You can download Docker from the [Docker website](https://www.docker.com),
or use the packaging system of your operating systems.

Use the `docker` command to create a container from the `trinodb/trino`
image. Assign it the `trino` name, to make it easier to reference it later.
Run it in the background, and map the default Trino port, which is 8080,
from inside the container to port 8080 on your workstation.

```text
docker run --name trino -d -p 8080:8080 trinodb/trino
```

Without specifying the container image tag, it defaults to `latest`,
but a number of any released Trino version can be used, for example
`trinodb/trino:|trino_version|`.

Run `docker ps` to see all the containers running in the background.

```text
% docker ps
CONTAINER ID   IMAGE               COMMAND                  CREATED        STATUS                  PORTS                    NAMES
955c3b3d3d0a   trinodb/trino:390   "/usr/lib/trino/bin/…"   39 hours ago   Up 39 hours (healthy)   0.0.0.0:8080->8080/tcp   trino
```

When Trino is still starting, it shows `(health: starting)`,
and `(healthy)` when it's ready.

:::{note}
There are multiple ways to use Trino within containers. You can either run
Trino in Docker containers locally, as explained in the following sections,
or use a container orchestration platform like Kubernetes. For the Kubernetes
instructions see {doc}`/installation/kubernetes`.
:::

## Executing queries

The image includes the Trino command-line interface (CLI) client, `trino`.
Execute it in the existing container to connect to the Trino server running
inside it. After starting the client, type and execute a query on a table
of the `tpch` catalog, which includes example data:

```text
$ docker exec -it trino trino
trino> select count(*) from tpch.sf1.nation;
 _col0
-------
    25
(1 row)

Query 20181105_001601_00002_e6r6y, FINISHED, 1 node
Splits: 21 total, 21 done (100.00%)
0:06 [25 rows, 0B] [4 rows/s, 0B/s]
```

Once you are done with your exploration, enter the `quit` command.

Alternatively, you can use the Trino CLI installed directly on your workstation.
The default server URL in the CLI of <http://localhost:8080> matches the port used
in the command to start the container. More information about using the CLI can
be found in {doc}`/client/cli`. You can also connect with any other client
application using the {doc}`/client/jdbc`.

## Configuring Trino

The image already contains a default configuration to get started, and some
catalogs to allow you to explore Trino. You can also use the container with your
custom configuration files in a local `etc` directory structure as created in
the {doc}`/installation/deployment`. If you mount this directory as a volume
in the path `/etc/trino` when starting the container, your configuration
is used instead of the default in the image.

```text
$ docker run --name trino -d -p 8080:8080 --volume $PWD/etc:/etc/trino trinodb/trino
```

To keep the default configuration and only configure catalogs, mount a folder
at `/etc/trino/catalog`, or individual catalog property files in it.

If you want to use additional plugins, mount them at `/usr/lib/trino/plugin`.

## Cleaning up

You can stop and start the container, using the `docker stop trino` and
`docker start trino` commands. To fully remove the stopped container, run
`docker rm trino`.
