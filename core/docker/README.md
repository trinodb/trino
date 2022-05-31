# Trino Docker Image

## About the Container
This Docker image is designed to provide the following
* An out-of-the-box single node cluster with the JMX, memory, TPC-DS, and TPC-H
 catalogs
* An image that can be deployed as a full cluster by mounting in configuration
* An image to be used as the basis for the Kubernetes Trino operator

## Quickstart

### Run the Trino server

You can launch a single node Trino cluster for testing purposes.
The Trino node will function both as a coordinator and a worker.
To launch it, execute the following:

```bash
docker run -p 8080:8080 --name trino trinodb/trino
```

Wait for the following message log line:
```
INFO	main	io.trino.server.Server	======== SERVER STARTED ========
```

The Trino server is now running on `localhost:8080` (the default port).

### Run the Trino CLI

Run the [Trino CLI](https://trino.io/docs/current/installation/cli.html),
which connects to `localhost:8080` by default:

```bash
docker exec -it trino trino
```

You can pass additional arguments to the Trino CLI:

```bash
docker exec -it trino trino --catalog tpch --schema sf1
```

## Configuration

Configuration is expected to be mounted `/etc/trino`. If it is not mounted
then the default single node configuration will be used.

### Specific Config Options

#### `node.id`

The container supplied `run-trino` command will set the config property
`node.id` to the hostname of the container if it is not specified in the
`node.properties` file. This allows for `node.properties` to be a static file
across all worker nodes if desired. Additionally this has the added benefit of
`node.id` being consistent, predictable, and stable through restarts.

#### `node.data-dir`

The default configuration uses `/data/trino` as the default for
`node.data-dir`. Thus if using the default configuration and a mounted volume
is desired for the data directory it should be mounted to `/data/trino`.

## Building a custom Docker image

To build an image for a locally modified version of Trino, run the Maven
build as normal for the `trino-server` and `trino-cli` modules, then
build the image:

```bash
./build.sh
```

The Docker build process will print the ID of the image, which will also
be tagged with `trino:xxx-SNAPSHOT`, where `xxx-SNAPSHOT` is the version
number of the Trino Maven build.

To build an image for a specific released version of Trino,
specify the `-r` option, and the build script will download
all the required artifacts:

```bash
./build.sh -r 381
```


## Getting Help

Join the Trino community [Slack](https://trino.io/slack.html).
