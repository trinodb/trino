# Trino Benchto benchmarks

The Benchto benchmarks utilize [Benchto](https://github.com/trinodb/benchto) benchmarking
utility to do macro benchmarking of Trino. As opposed to micro benchmarking which exercises
a class or a small, coherent set of classes, macro benchmarks done with Benchto use Trino
end-to-end, by accessing it through its API (usually with `trino-jdbc`), executing queries,
measuring time and gathering various metrics.

## Benchmarking suites

Even though benchmarks exercise Trino end-to-end, a single benchmark cannot use all Trino
features. Therefore benchmarks are organized in suites, like:

* *tpch* - queries closely following the [TPC-H](http://www.tpc.org/tpch/) benchmark
* *tpcds* - queries closely following the [TPC-DS](http://www.tpc.org/tpcds/) benchmark

## Usage

### Requirements

* Trino already installed on the target environment
* Basic understanding of Benchto [components and architecture](https://github.com/trinodb/benchto)
* Benchto service [configured and running](https://github.com/trinodb/benchto/tree/master/benchto-service)
* An environment [defined in Benchto service](https://github.com/trinodb/benchto/tree/master/benchto-service#creating-environment)

### Configuring benchmarks

Benchto driver needs to know two things: what benchmark is to be run and what environment
it is to be run on. For the purpose of the following example, we will use `tpch` benchmark
and Trino server running at `localhost:8080`, with Benchto service running at `localhost:8081`.

Benchto driver uses Spring Boot to locate environment configuration file, so to pass the
configuration. To continue with our example, one needs to place an `application.yaml`
file in the current directory (i.e. the directory from which the benchmark will be invoked),
with the following content:

```yaml
benchmarks: src/main/resources/benchmarks
sql: sql/main/resources/sql
query-results-dir: target/results

benchmark-service:
  url: http://localhost:8081

data-sources:
  trino:
    url: jdbc:trino://localhost:8080
    username: na
    driver-class-name: io.trino.jdbc.TrinoDriver

environment:
  name: TRINO-DEV

presto:
  url: http://localhost:8080
  username: na

benchmark:
  feature:
    trino:
      metrics.collection.enabled: true

macros:
  sleep-4s:
    command: echo "Sleeping for 4s" && sleep 4
```

### Bootstrapping benchmark data

* Make sure you have configured [Trino TPC-H connector](https://trino.io/docs/current/connector/tpch.html).
* Bootstrap benchmark data:
  ```bash
  testing/trino-benchto-benchmarks/generate_schemas/generate-tpch.py --factors sf1 --formats orc | trino-cli-[version]-executable.jar --server [trino_coordinator-url]:[port]
  ```

### Configuring overrides file

It is possible to override benchmark variables with benchto-driver overrides feature.
This is useful for instance when one wants to use different number of benchmark
runs or different underlying schemas. Create a simple `overrides.yaml` file:

```yaml
runs: 10
tpch_300: tpch_sf1_orc
scale_300: 1
tpch_1000: tpch_sf1_orc
scale_1000: 1
tpch_3000: tpch_sf1_orc
scale_3000: 1
prefix: ""
```

### Running benchto-driver

With the scene set up as in the previous section, the benchmark can be run with:
```bash
./mvnw clean package -pl :trino-benchto-benchmarks
java -jar "$HOME/.m2/repository/io/trino/benchto/benchto-driver/0.20/benchto-driver-0.20-exec.jar" \
            --activeBenchmarks=trino/tpch \
            --overrides "overrides.yaml"
```
