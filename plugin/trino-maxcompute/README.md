2020年12月27日[Presto SQL更名](https://trino.io/blog/2020/12/27/announcing-trino.html)为Trino。 Trino-Odps Connector作为Trino的一个Plugin，可以支持通过Trino SQL来访问Odps表的数据，并且可以结合其他Catalog的数据进行联邦查询，下面将介绍下这个Plugin的部署和使用。

## 部署步骤
Trino的部署可以参考官方文档：https://trino.io/docs/current/installation/deployment.html 为了简单起见，下面以部署一个单节点Trino Cluster为例来看下如何在Trino中加入Odps Connector。

1、下载364的Trino版本（当前只在该版本验证过，相近版本应该也是可以支持的）：

```shell
# trino-server-364，也可以通过浏览器下载
wget https://repo1.maven.org/maven2/io/trino/trino-server/364/trino-server-364.tar.gz

# 解压下载后的Trino包：
tar zxvf trino-server-364.tar.gz
```

2、编译并部署Trino-ODPS Connector的Plugin jar包

```shell
# trino 364 基于java 11
export JAVA_HOME=${jdk-11.0.11}

# 需要注意PATH中不要包含低版本的java
export PATH=${JAVA_HOME}:$PATH

# 进入trino-connector项目根目录
cd ${workspace}/trino-connector

# 打包trino-ODPS connector
mvn clean package

# 部署connector
mkdir ${trino-server-364}/plugin/odps
cp ${workspace}/trino-connector/target/trino-odps-connector-${version}.jar ${trino-server-364}/plugin/odps/
cp ${workspace}/trino-connector/libs/*.jar ${trino-server-364}/plugin/odps/
```

3、配置Trino
在解压后的trino-server-364目录下创建etc目录，etc目录里面分别需要创建config.properties，jvm.config，log.properties，node.properties以及catalog目录。
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

- config.properties 内容如下
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

- jvm.config 内容如下
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

- log.properties 内容如下
```text
io.trino=INFO
```

- node.properties 内容如下
```text
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-fffffffffffe
node.data-dir=/path/to/trino-server-364/data
```

- odps.properties 内容如下：
```text
connector.name=odps
odps.project.name=XXXXXX
odps.access.id=XXXXXX
odps.access.key=XXXXXX
odps.end.point=XXXXXX
odps.input.split.size=64
```
> 注意：
> - odps.end.point和odps.tunnel.end.point的参数配置具体可以参考文档：https://help.aliyun.com/document_detail/34951.html ，根据project所在的region以及presto运行所在的网络环境来配置。
> - 如果需要同时访问多个project，通过etc/catalog/odps.properties新增的参数来配置，odps.project.name.extra.list=cupid_test,xxx,aaaas  这样逗号分隔，配置多个。

4、启动Trino

```shell
# trino 364 基于java 11
export JAVA_HOME=${jdk-11.0.11}
# 需要注意PATH中不要包含低版本的java
export PATH=${JAVA_HOME}:$PATH
```

上面这些配置都完成后，就可以启动Trino了。

- bin/launcher start 这样会把Trino放到后台运行
- bin/launcher run 这样是在前台运行

## 使用方式
下面介绍通过Trino CLI的方式来连接使用Odps Catalog.
1、下载Trino CLI

```shell
wget https://repo1.maven.org/maven2/io/trino/trino-cli/364/trino-cli-364-executable.jar
```

下载后将trino-cli-364-executable.jar重命名为trino，并且加上可执行的权限。
```shell
mv trino-cli-364-executable.jar trino
chmod +x trino
```

2、运行 Trino CLI
通过如下命令即可连接上Odps的Catalog进行使用。

```shell
./trino --server 127.0.0.1:8080 --catalog odps --schema project_name
```

在Trino CLI成功连接后，可以使用Trino的SQL对Odps表数据进行查询，例如：

```sql
show tables;
desc tablename;
select * from xxx limit 10;
select count(*) from xxx;
```

限制条件
- 不支持读取array,map,struct等复杂类型的表数据
- 不支持将数据写入odps表
