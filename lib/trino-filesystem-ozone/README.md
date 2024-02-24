# PoC only. Use at your own risk.

https://github.com/apache/ozone/pull/5993
Needs `--add-opens=java.base/java.nio=ALL-UNNAMED`

# Overall
```
Legacy (File system style access):
TrinoFileSystem -> HdfsFileSystem -> OzoneFileSystem -> Ozone RPC

Native (Object storage style access):
TrinoFileSystem -> Ozone RPC
```

Changes:
* Do not support rename()
* Some configs in `ozone-site.xml` won't work


# Memo

## Ozone
Ozone protocols:
```
S3 client --S3 Prot--> S3 Gateway -> Java API (Ozone Bucket, RpcClient...) -> OM
HDFS client -> OzoneFileSystem    -> Java API (Ozone Bucket, RpcClient...) -> OM
                                  -> Java API (Ozone Bucket, RpcClient...) -> OM
```

Java API
```
Bucket.readKey()
  org.apache.hadoop.ozone.client.rpc.RpcClient.getInputStreamWithRetryFunction
   createInputStream
    org.apache.hadoop.ozone.client.io.KeyInputStream.createStreams
     https://github.com/apache/ozone/blob/c623e942c112c45918047ed5d339c183c6f7d58c/hadoop-ozone/client/src/main/java/org/apache/hadoop/ozone/client/io/KeyInputStream.java#L88
```

hadoop.fs.FileSystem
```
org.apache.hadoop.fs.ozone.OzoneFileSystem
public InputStream readFile(String key) throws IOException
https://github.com/apache/ozone/blob/5635e5e4e689ea0fcee978812dffb6e227fabfd5/hadoop-ozone/ozonefs-common/src/main/java/org/apache/hadoop/fs/ozone/BasicOzoneClientAdapterImpl.java#L226
-> bucket.readFile(key)
```

Advantages of using Ozone Java API:
* Get rid of hadoop.fs.FileSystem & OzoneFileSystem
* Remove the necessary(?) features by OzoneFileSystem
* Can directly use Java API, no need to use FileSystem abstraction
  * What's the actual benefit? => What can be done by Java API but not FileSystem?
    * FSDataInputStream(OzoneInputStream + statics) vs OzoneInputStream
  * May be security setting?
* OpenTelemetry support?

Disadvantages:
* Need to handle the schema path to schema (o3, o3fs, ofs...).
* Ozone config won't work? (ozone-site.xml)
  * Need to use Trino properties config file
* Potentially reimplementing all the OzoneFileSystem?


Memo:
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
* Ozone failed on https://github.com/trinodb/trino/blob/30348401c447691237c57a5f6d6c95eef87560fc/lib/trino-filesystem/src/test/java/io/trino/filesystem/AbstractTestTrinoFileSystem.java#L1258
"java.io.UncheckedIOException: NOT_A_FILE org.apache.hadoop.ozone.om.exceptions.OMException: Can not create file: bucket1/level0/level1 as there is already file in the given path"

Trino FS related:
* https://github.com/trinodb/trino/blob/4829f69019029c521c8454161dfe278dc267a662/lib/trino-filesystem-s3/src/main/java/io/trino/filesystem/s3/S3OutputFile.java#L43
is this assumption true? Gcs/Azure/HDFS/LocalFS return FileAlreadyExistsException. S3 is always Overwrite.
* deleteFile() { location.verifyValidFileLocation(); }exist in azure and s3, but not gcs
* which ops are not required to impl for object storaged type of access is not clearly defined
renameFile() is implemented for azure, but not s3 and gcs
directoryExists() should return Optional.empty() for blob according to comment, but s3/gcs/azure all do a list file ops. Is this needed?


# Setup Ozone using docker compose
docker run apache/ozone cat docker-compose.yaml > docker-compose.yaml
docker run apache/ozone cat docker-config > docker-config
docker compose up -d

Add ports to om
```
      ports:
         - 9862:9862
```
# Setup Ozone using minikube
This didn't work. Trino cannot connecto to data node because they only have k8s internal ip.  

export AWS_ACCESS_KEY_ID=testuser/scm@EXAMPLE.COM
export AWS_SECRET_ACCESS_KEY=xyz

aws s3api --endpoint http://localhost:9878/ delete-bucket --bucket=bucket1
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1
ls -1 > /tmp/testfile

aws s3api --endpoint http://localhost:9878/ put-object --bucket bucket1 --key s3://bucket1/testfile --body /tmp/testfile
aws s3api --endpoint http://localhost:9878/ put-object --bucket bucket1 --key s3://bucket1/testfile/test --body /tmp/testfile


aws s3api --endpoint http://localhost:9878/ list-objects --bucket bucket1
```
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
(
```



➜  minikube service s3g-public
|-----------|------------|-------------|---------------------------|
| NAMESPACE |    NAME    | TARGET PORT |            URL            |
|-----------|------------|-------------|---------------------------|
| default   | s3g-public | rest/9878   | http://localhost:9878/ |
|-----------|------------|-------------|---------------------------|
🎉  Opening service default/s3g-public in default browser...
➜  minikube Gtk-Message: 22:09:50.897: Not loading module "atk-bridge": The functionality is provided by GTK natively. Please try to not load it.

➜  minikube service om-public
|-----------|-----------|----------------|---------------------------|
| NAMESPACE |   NAME    |  TARGET PORT   |            URL            |
|-----------|-----------|----------------|---------------------------|
| default   | om-public | ui/9874        | http://192.168.49.2:31849 |
|           |           | hadooprpc/9862 | http://192.168.49.2:31193 |
|-----------|-----------|----------------|---------------------------|
[default om-public ui/9874
hadooprpc/9862 http://192.168.49.2:31849
http://192.168.49.2:31193]


# TODO
What's the diff between readFile() and readKey()?
=> looks like readFile is newer, is optimized and better?
=> fs.open() is using readFile
for blob storage should use key i guess
```
  public OzoneInputStream readFile(String volumeName, String bucketName,
      String keyName) throws IOException {
  public OzoneInputStream getKey(
      String volumeName, String bucketName, String keyName)
      throws IOException {
  }
  
  /**
   * OzoneFS api to creates an input stream for a file.
   *
   * @param keyName Key name
   * @throws OMException if given key is not found or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneInputStream readFile(String keyName) throws IOException {
    return proxy.readFile(volumeName, name, keyName);
  }

  /**
   * Reads an existing key from the bucket.
   *
   * @param key Name of the key to be read.
   * @return OzoneInputStream the stream using which the data can be read.
   * @throws IOException
   */
  public OzoneInputStream readKey(String key) throws IOException {
    return proxy.getKey(volumeName, name, key);
  }

```

---

```
AbstractS3
    protected final Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucket()));
    }
S3Minio
  bucket = "test-bucket-test-s3-file-system-minio";
S3Mock
  bucket = "test-bucket"
TestS3FileSystemLocalStack
  BUCKET = "test-bucket";
AwsS3
  bucket = environmentVariable("EMPTY_S3_BUCKET");
  
AbstractGcs
this.rootLocation = Location.of("gs://%s/".formatted(bucket));
bucketName =  BUCKET_NAME_PREFIX "gcloud-test-bucket-temp-" + UUID.randomUUID().toString();

AbstractTestAzureFileSystem
        containerName = "test-%s-%s".formatted(accountKind.name().toLowerCase(ROOT), randomUUID());
        rootLocation = Location.of("abfs://%s@%s.dfs.core.windows.net/".formatted(containerName, account));
        
AbstractOzone
        this.rootLocation = Location.of("o3://%s/%s/".formatted("s3v", "bucket1"));
```

```
tempBlob.location = "o3://s3v/bucket1/test/.././/file"

fs.createOrOverwrite(TEST_BLOB_CONTENT_PREFIX "test blob content for " + tempBlob.location().toString());

"test blob content for o3://s3v/bucket1/test/.././/file"

{
    "Contents": [
        {
            "Key": "bucket1/test/.././/file",
            "LastModified": "2024-02-24T06:44:32.579000+00:00",
            "ETag": "2024-02-24T06:44:32.579Z",
            "Size": 54,
            "StorageClass": "REDUCED_REDUNDANCY"
        }
    ],
    "RequestCharged": null
}

!normalizesListFilesResult() = true ??
assertThat(listPath("test/..")).containsExactly(tempBlob.location());

path = "test/.."
fs.listFiles("test/..")

fileEntry.location = "o3://bucket1/bucket1/test/.././/file"
fileEntry.length() == 54
```

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
