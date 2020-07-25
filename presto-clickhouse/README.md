# Presto ClickHouse Connector

This is a plugin for Presto that allow you to use ClickHouse Jdbc Connection

[![Presto-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](http://presto-connectors.ml)

## compile

```shell
mvn clean package
```

## install

1. create dir named `clickhouse` in your presto plugin directory(defaults to `/usr/lib/presto/plugin`),
2. copy `presto-clickhouse-338-services.jar` and `target/presto-clickhouse-338.jar` to `/usr/lib/presto/shared`
3. copy lastest [clickhouse-jdbc.jar](https://github.com/ClickHouse/clickhouse-jdbc/releases/download/release_0.2.4/clickhouse-jdbc-0.2.4-shaded.jar) to `/usr/lib/presto/shared`
4. run the following commands to create necessary symbol links

    ```shell
    cd /usr/lib/presto/plugin/clickhouse
    ln -sf ../../shared/aopalliance-1.0.jar  .
    ln -sf ../../shared/bootstrap-0.198.jar  .
    ln -sf ../../shared/bval-jsr-2.0.0.jar  .
    ln -sf ../../shared/cglib-nodep-3.3.0.jar  .
    ln -sf ../../shared/checker-qual-2.11.1.jar  .
    ln -sf ../../shared/clickhouse-jdbc-0.2.4.jar  .
    ln -sf ../../shared/concurrent-0.198.jar  .
    ln -sf ../../shared/configuration-0.198.jar  .
    ln -sf ../../shared/error_prone_annotations-2.3.4.jar  .
    ln -sf ../../shared/failureaccess-1.0.1.jar  .
    ln -sf ../../shared/guava-29.0-jre.jar  .
    ln -sf ../../shared/guice-4.2.3.jar  .
    ln -sf ../../shared/HdrHistogram-2.1.9.jar  .
    ln -sf ../../shared/j2objc-annotations-1.3.jar  .
    ln -sf ../../shared/jackson-core-2.10.4.jar  .
    ln -sf ../../shared/jackson-databind-2.10.4.jar  .
    ln -sf ../../shared/jackson-datatype-guava-2.10.4.jar  .
    ln -sf ../../shared/jackson-datatype-jdk8-2.10.4.jar  .
    ln -sf ../../shared/jackson-datatype-joda-2.10.4.jar  .
    ln -sf ../../shared/jackson-datatype-jsr310-2.10.4.jar  .
    ln -sf ../../shared/jackson-module-parameter-names-2.10.4.jar  .
    ln -sf ../../shared/javax.annotation-api-1.3.2.jar  .
    ln -sf ../../shared/javax.inject-1.jar  .
    ln -sf ../../shared/jcl-over-slf4j-1.7.30.jar  .
    ln -sf ../../shared/jmxutils-1.21.jar  .
    ln -sf ../../shared/joda-time-2.10.5.jar  .
    ln -sf ../../shared/json-0.198.jar  .
    ln -sf ../../shared/jsr305-3.0.2.jar  .
    ln -sf ../../shared/log-0.198.jar  .
    ln -sf ../../shared/log4j-over-slf4j-1.7.30.jar  .
    ln -sf ../../shared/logback-core-1.2.3.jar  .
    ln -sf ../../shared/log-manager-0.198.jar  .
    ln -sf ../../shared/ons-19.3.0.0.jar  .
    ln -sf ../../shared/osdt_cert-19.3.0.0.jar  .
    ln -sf ../../shared/osdt_core-19.3.0.0.jar  .
    ln -sf ../../shared/parameternames-1.4.jar  .
    ln -sf ../../shared/presto-base-jdbc-338.jar  .
    ln -sf ../../shared/presto-clickhouse-338.jar  .
    ln -sf ../../shared/presto-clickhouse-338-services.jar  .
    ln -sf ../../shared/presto-matching-338.jar  .
    ln -sf ../../shared/presto-plugin-toolkit-338.jar  .
    ln -sf ../../shared/simplefan-19.3.0.0.jar  .
    ln -sf ../../shared/slf4j-api-1.7.30.jar  .
    ln -sf ../../shared/slf4j-jdk14-1.7.30.jar  .
    ln -sf ../../shared/stats-0.198.jar  .
    ln -sf ../../shared/ucp-19.3.0.0.jar  .
    ln -sf ../../shared/units-1.6.jar  .
    ln -sf ../../shared/validation-api-2.0.1.Final.jar  .
    ln -sf ../../shared/httpclient-4.5.9.jar .
    ln -sf ../../shared/httpcore-4.4.6.jar .
    ln -sf ../../shared/lz4-java-1.6.0.jar .
    ```

## Connection Configuration

Create new properties file inside etc/catalog dir:

```ini
connector.name=clickhouse
connection-url=jdbc:clickhouse://ip:port/database
connection-user=myuser
connection-password=
```