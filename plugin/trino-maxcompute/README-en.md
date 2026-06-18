PrestorSQL community [rebranding PrestoSQL as Trino](https://trino.io/blog/2020/12/27/announcing-trino.html) on Dec 27, 2020。 As a Plugin of Trino, Trino-Odps Connectorcan support access to Odps table data through Trino SQL，and can combine data from other catalogs for federated query. The following will introduce the deployment and usage of this Plugin.

## Deploying Trino

For the deployment of Trino, refer to the official documentation: https://trino.io/docs/current/installation/deployment.html. For simplicity, let's take the deployment of a single-node Trino Cluster as an example to see how to add Odps Connector to Trino. 


1、Download the Trino version 364. Currently it has only been verified in this version, and similar versions should also be supported.

```shell
# download trino 
wget https://repo1.maven.org/maven2/io/trino/trino-server/364/trino-server-364.tar.gz

# unzip tar package
tar zxvf trino-server-364.tar.gz
```

2、Compile and deploying the Plugin jar package

```shell
# java 11 is requirement of running trino
export JAVA_HOME=${jdk-11.0.11}

# exclude java elder version 
export PATH=${JAVA_HOME}:$PATH

# enter root director of trino-connector project
cd ${workspace}/trino-connector

# package connector
mvn clean package

# deploy connector
mkdir ${trino-server-364}/plugin/odps
cp ${workspace}/trino-connector/target/trino-odps-connector-${version}.jar ${trino-server-364}/plugin/odps/
cp ${workspace}/trino-connector/libs/*.jar ${trino-server-364}/plugin/odps/
```

3、Configure Trino

Create the sub-directory etc in the decompressed trino-server-364 directory. In the etc directory, you need to create the config.properties, jvm.config, log.properties, node.properties and catalog directories respectively.

```text
${trino-server-364}
  |
  +-- config.properties
  |
  +-- jvm.config
  |
  +-- log.properties
  |
  +-- node.properties
  |
  +-- catalog
       |
       +-- odps.properties

```

- The content of config.properties
```text
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=5GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB
discovery-server.enabled=true
discovery.uri=http://127.0.0.1:8080
```

- The content of jvm.config 
```text
-server
-Xmx16G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
```

- The content of log.properties
```text
io.trino=INFO
```

- The content of node.properties
```text
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-fffffffffffe
node.data-dir=/path/to/trino-server-364/data
```

- The content of odps.properties
```text
connector.name=odps
odps.project.name=XXXXXX
odps.access.id=XXXXXX
odps.access.key=XXXXXX
odps.end.point=XXXXXX
odps.input.split.size=64
```

> Notice：
> - For details of configuration items both odps.end.point and odps.tunnel.end.point, please refer to the document:https://help.aliyun.com/document_detail/34951.html. 
> - If you need to access multiple projects at the same time, configure them through the newly added parameters in etc/catalog/odps.properties, such as odps.project.name.extra.list=cupid_test,xxx,aaaas separated by commas.

4、Start Trino

```shell
# java 11 is requirement of running trino
export JAVA_HOME=${jdk-11.0.11}

# exclude java elder version 
export PATH=${JAVA_HOME}:$PATH
```

After the above configuration is completed, you can start Trino.

- `bin/launcher start` start Trino as daemon service
- `bin/launcher run` start Trino as foreground process

## Usage
The following describes how to connect and use Odps Catalog through Trino CLI.

1、Download Trino CLI

```shell
wget https://repo1.maven.org/maven2/io/trino/trino-cli/364/trino-cli-364-executable.jar
```

After downloading, rename trino-cli-364-executable.jar to trino and add executable permissions.
```shell
mv trino-cli-364-executable.jar trino
chmod +x trino
```

2、Run the Trino CLI

Use the following command to connect to the Odps Catalog for use.

```shell
./trino --server 127.0.0.1:8080 --catalog odps --schema project_name
```

After the Trino CLI is successfully connected, you can use the Trino SQL to query the Odps table data, for example:

```sql
show tables;
desc tablename;
select * from xxx limit 10;
select count(*) from xxx;
```

Limitation
- It does not support reading table data of complex types such as array, map, struct, etc.
- Writing data to odps table is not supported
