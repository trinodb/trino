# presto-docker-image

## About the Container
This Docker image is designed to provide the following
* An out-of-the-box single node cluster with the JMX, memory, TPC-DS, and TPC-H
 catalogs
* An image that can be deployed as a full cluster by mounting in configuration
* An image to be used as the basis for the Kubernetes Presto operator

## Configuration

Configuration is expected to be mounted to either  to `/etc/presto` or
`/usr/lib/presto/etc` (the latter takes precedence). If neither of these exists
then the default single node configuration will be used.

### Quickstart
Use the following guide to quickly launch single-node Presto installation in a Docker container.

 1. Run the Presto docker

  You can launch single-node Presto for testing purposes. The Presto node will function both as a coordinator and a worker. To launch it, execute:

  ```bash
  docker run -p 8080:8080 --name presto prestosql/presto
  ```
  
  NOTE: You need to have [docker](https://docs.docker.com/install/) installed in the system

  Wait for the message: `INFO	main	io.prestosql.server.PrestoServer	======== SERVER STARTED ========`

  Now Presto server is running at `localhost:8080`
 
 2. Connect to Presto 
 
  To connect to Presto server: 
  ```bash
  docker exec -it presto presto
  ```	 	

  Now, Show the catalogs using:	
  ```bash
  presto> show catalogs;
  ```
  Show schemas in catalog `tpch`:
  
  Use catalog tpch and schema sf1: 
  ```bash
  presto:sf1> use tpch.sf1;
  ```

  OR  
  
  ```
  docker exec -it presto presto --catalog tpch --schema sf1
  ```
  
  Run the queries:
  ```bash
  presto:sf1> SELECT count(*) FROM customer;
  ```
  
  For more details on CLI check out the [docs](https://prestosql.io/docs/current/installation/cli.html)

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


### Building Custom Docker Image

To build images for custom versions of presto or as a base for a custom image of prestosql, RUN `./build-remote.sh VersionNumber`

Example: 
```bash
git clone https://github.com/prestosql/presto
cd presto/presto-docker-image
./build-remote.sh 312
```
