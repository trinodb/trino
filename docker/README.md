# Presto Docker Image

## About the Container
This Docker image is designed to provide the following
* An out-of-the-box single node cluster with the JMX, memory, TPC-DS, and TPC-H
 catalogs
* An image that can be deployed as a full cluster by mounting in configuration
* An image to be used as the basis for the Kubernetes Presto operator

## Quickstart

### Run the Presto server

You can launch a single node Presto cluster for testing purposes.
The Presto node will function both as a coordinator and a worker.
To launch it, execute the following:

```bash
docker run -p 8080:8080 --name presto prestosql/presto
```

Wait for the following message log line:
```
INFO	main	io.prestosql.server.PrestoServer	======== SERVER STARTED ========
```

The Presto server is now running on `localhost:8080` (the default port).

### Run the Presto CLI

Run the [Presto CLI](https://prestosql.io/docs/current/installation/cli.html),
which connects to `localhost:8080` by default:

```bash
docker exec -it presto presto
```

You can pass additional arguments to the Presto CLI:

```bash
docker exec -it presto presto --catalog tpch --schema sf1
```

## Configuration

Configuration is expected to be mounted to either  to `/etc/presto` or
`/usr/lib/presto/etc` (the latter takes precedence). If neither of these exists
then the default single node configuration will be used.

### Specific Config Options

#### `node.id`
The container supplied `run-presto` command will set the config property
`node.id` to the hostname of the container if it is not specified in the
`node.properties` file. This allows for `node.properties` to be a static file
across all worker nodes if desired. Additionally this has the added benefit of
`node.id` being consistent, predictable, and stable through restarts.

#### `node.data-dir`
The default configuration uses `/data/presto` as the default for
`node.data-dir`. Thus if using the default configuration and a mounted volume
is desired for the data directory it should be mounted to `/data/presto`.

## Building a custom Docker image

To build an image for a locally modified version of Presto, run the Maven
build as normal for the `presto-server` and `presto-cli` modules, then
build the image:

```bash
./build-local.sh
```

The Docker build process will print the ID of the image, which will also
be tagged with `presto:xxx-SNAPSHOT`, where `xxx-SNAPSHOT` is the version
number of the Presto Maven build.

## Getting Help

Join the Presto community [Slack](https://prestosql.io/slack.html).
