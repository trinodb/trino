# PoC only. Use at your own risk.

# Overall
Existing:
```
Legacy (File system style access):
TrinoFileSystem -> HdfsFileSystem -> OzoneFileSystem -> Ozone RPC -----> Ozone Manager(OM)

Legacy (Blob storage style access) + Ozone S3G:
TrinoFileSystem -> HdfsFileSystem -> s3a -----> Ozone S3 Gateway -> Ozone Manager(OM)

Native (Blob storage style access) + Ozone S3G:
TrinoFileSystem -> S3FileSystem -> s3a -----> Ozone S3 Gateway -> Ozone Manager(OM)
```

PoC:
```
Native (Blob storage style access):
TrinoFileSystem -> Ozone RPC -----> Ozone Manager(OM)
```

# How to test
## Setup Ozone
Ozone with minikube didn't work. Trino cannot connect to data node because they only have k8s internal ip.
Use docker compose: https://ozone.apache.org/docs/current/start/runningviadocker.html
```
docker run apache/ozone cat docker-compose.yaml > docker-compose.yaml
docker run apache/ozone cat docker-config > docker-config
```

Edit `docker-compose.yaml`, add RPC ports to om.
```
      ports:
         - 9862:9862
```

```
docker compose up -d
```

## Setup buckets
```
export AWS_ACCESS_KEY_ID=testuser/scm@EXAMPLE.COM
export AWS_SECRET_ACCESS_KEY=xyz

aws s3api --endpoint http://localhost:9878/ delete-bucket --bucket=bucket1
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1

ls -1 > /tmp/testfile
aws s3api --endpoint http://localhost:9878/ put-object --bucket bucket1 --key s3://bucket1/testfile --body /tmp/testfile
aws s3api --endpoint http://localhost:9878/ list-objects --bucket bucket1
{
    "Contents": [
        {
            "Key": "s3://bucket1/testfile",
            "LastModified": "2024-02-22T11:20:13.038000+00:00",
            "ETag": "2024-02-22T11:20:13.038Z",
            "Size": 246,
            "StorageClass": "STANDARD"
        }
    ],
    "RequestCharged": null
}
```

# Memo

## Ozone
### Java API
Looks like there are two types of APIs in `org.apache.hadoop.ozone.client.OzoneBucket`.
One is blob storage(getKey/listKeys...) and one is for hierarchical file systems(readFile/listStatus...).


```
Bucket.readKey()
  org.apache.hadoop.ozone.client.rpc.RpcClient.getInputStreamWithRetryFunction
   createInputStream
    org.apache.hadoop.ozone.client.io.KeyInputStream.createStreams
     https://github.com/apache/ozone/blob/c623e942c112c45918047ed5d339c183c6f7d58c/hadoop-ozone/client/src/main/java/org/apache/hadoop/ozone/client/io/KeyInputStream.java#L88
```

```
org.apache.hadoop.fs.ozone.OzoneFileSystem implements hadoop.fs.FileSystem
public InputStream readFile(String key) throws IOException
https://github.com/apache/ozone/blob/5635e5e4e689ea0fcee978812dffb6e227fabfd5/hadoop-ozone/ozonefs-common/src/main/java/org/apache/hadoop/fs/ozone/BasicOzoneClientAdapterImpl.java#L226
-> bucket.readFile(key)
```

Advantages of using Trino Native FS + Ozone Java API:
* Get rid of hadoop.fs.FileSystem & OzoneFileSystem
  * Can directly use Java API, no need to use FileSystem abstraction
    * What's the actual benefit? => What can be done by Java API but not FileSystem?
      * FSDataInputStream(OzoneInputStream + statics) vs OzoneInputStream
  * OpenTelemetry support
  * Use blob storage API instead of hierarchical file systems AI

Disadvantages:
* Some of the Ozone config won't work? (ozone-site.xml)
  * Need to use Trino properties config file
* Potentially reimplementing the OzoneFileSystem


## Implementation memo
* Rename support?: https://ozone.apache.org/docs/current/feature/prefixfso.html
  * What's the disadvantages of turning on this feature
  * https://issues.apache.org/jira/browse/HDDS-2939
    * need for path traversal in the Directory table whenever a pathname needs to be looked up
    * op amplification during create operation - the need to insert new directories into the table
    * locking and limited concurrency compared to object store key management
* OzoneInputFile/OzoneTrinoInputStream is just a copy of HdfsInputFile/HdfsTrinoInputStream, they are almost identical
* https://github.com/apache/ozone/blob/ozone-1.4.0/hadoop-hdds/docs/content/start/Minikube.md should be update -> `kubectl apply -k .`?
* How to handle those schemas?
  * o3fs and ofs are hadoop compatible
  * ofs://      => supports operations across all volumes and buckets and provides a full view of all the volume/buckets
  * o3fs://     => supports operations only at a single bucket
  * S3 Protocol => can only access "/s3v" volume

---

Trino FS related:
* https://github.com/trinodb/trino/blob/4829f69019029c521c8454161dfe278dc267a662/lib/trino-filesystem-s3/src/main/java/io/trino/filesystem/s3/S3OutputFile.java#L43
is this assumption true? Gcs/Azure/HDFS/LocalFS return FileAlreadyExistsException. S3 is always Overwrite.
* deleteFile() { location.verifyValidFileLocation(); } exist in azure and s3, but not gcs(use checkIsValidFile)
* renameFile() is implemented for azure, but not s3 and gcs

---

Ozone schema:
```
ofs://om1/
ofs://om3:9862/
ofs://omservice/
ofs://omservice/volume1/
ofs://omservice/volume1/bucket1/
ofs://omservice/volume1/bucket1/dir1
ofs://omservice/volume1/bucket1/dir1/key1

ofs://omservice/tmp/
ofs://omservice/tmp/key1
ofs://om-host.example.com
<property>
  <name>fs.defaultFS</name>
  <value>ofs://omservice</value>
</property>
ozone fs -mkdir -p /volume1/bucket1
ozone fs -mkdir -p ofs://omservice/volume1/bucket1/dir1/
---
<property>
  <name>fs.defaultFS</name>
  <value>o3fs://bucket.volume</value>
</property>
o3fs://bucket.volume.om-host.example.com:5678/key

hdfs dfs -ls o3fs://bucket.volume.om-host.example.com/key
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:6789/key
```

---

NullPointerException when testing listDirectory "/" and shadow=true
`bucket.listKeys("/", null, true);`
```
2024-02-24 12:07:19 WARN  Server:3107 - IPC Server handler 74 on default port 9862, call Call#990 Retry#8 org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol.submitRequest from 172.18.0.1:48850 / 172.18.0.1:48850
java.lang.NullPointerException
	at org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.isFile(OzoneFSUtils.java:92)
	at org.apache.hadoop.ozone.om.KeyManagerImpl.findKeyInDbWithIterator(KeyManagerImpl.java:1707)
	at org.apache.hadoop.ozone.om.KeyManagerImpl.listStatus(KeyManagerImpl.java:1607)
	at org.apache.hadoop.ozone.om.OmMetadataReader.listStatus(OmMetadataReader.java:240)
	at org.apache.hadoop.ozone.om.OzoneManager.listStatus(OzoneManager.java:3672)
	at org.apache.hadoop.ozone.om.OzoneManager.listStatusLight(OzoneManager.java:3682)
	at org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler.listStatusLight(OzoneManagerRequestHandler.java:1243)
	at org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler.handleReadRequest(OzoneManagerRequestHandler.java:282)
	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.submitReadRequestToOM(OzoneManagerProtocolServerSideTranslatorPB.java:265)
	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.internalProcessRequest(OzoneManagerProtocolServerSideTranslatorPB.java:211)
	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.processRequest(OzoneManagerProtocolServerSideTranslatorPB.java:171)
	at org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher.processRequest(OzoneProtocolMessageDispatcher.java:89)
	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.submitRequest(OzoneManagerProtocolServerSideTranslatorPB.java:162)
	at org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos$OzoneManagerService$2.callBlockingMethod(OzoneManagerProtocolProtos.java)
	at org.apache.hadoop.ipc.ProtobufRpcEngine$Server.processCall(ProtobufRpcEngine.java:484)
	at org.apache.hadoop.ipc.ProtobufRpcEngine2$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine2.java:595)
	at org.apache.hadoop.ipc.ProtobufRpcEngine2$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine2.java:573)
	at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:1227)
	at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:1094)
	at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:1017)
	at java.base/java.security.AccessController.doPrivileged(Native Method)
	at java.base/javax.security.auth.Subject.doAs(Subject.java:423)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1899)
	at org.apache.hadoop.ipc.Server$Handler.run(Server.java:3048)
```

---

io.trino.filesystem.AbstractTestTrinoFileSystem#testDirectoryExists Error:
Can append path in o3://, because path contains bucket.

Location loc1 = "o3://vol/bucket/foo/file1"
Location loc2 = "o3://vol/bucket

String path1 = loc1.path()  // path1 = "bucket/foo/file1"
loc2.appendPath(loc1.path)  // "o3://vol/bucket/bucket/foo/file1"

PS:
Let's say the rootLocation is `/work`.
In `/work` there is file a file foo (`/work/foo`)
createLocation(foo.path()) doesn't make sense
