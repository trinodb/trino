# Hive connector with IBM Cloud Object Storage

Configure the {doc}`hive` to support [IBM Cloud Object Storage COS](https://www.ibm.com/cloud/object-storage) access.

## Configuration

To use COS, you need to configure a catalog file to use the Hive
connector. For example, create a file `etc/ibmcos.properties` and
specify the path to the COS service config file with the
`hive.cos.service-config` property.

```properties
connector.name=hive
hive.cos.service-config=etc/cos-service.properties
```

The service configuration file contains the access and secret keys, as well as
the endpoints for one or multiple COS services:

```properties
service1.access-key=<your-access-key1>
service1.secret-key=<your-secret-key1>
service1.endpoint=<endpoint1>
service2.access-key=<your-access-key2>
service2.secret-key=<your-secret-key2>
service2.endpoint=<endpoint2>
```

The endpoints property is optional. `service1` and `service2` are
placeholders for unique COS service names. The granularity for providing access
credentials is at the COS service level.

To use IBM COS service, specify the service name, for example: `service1` in
the COS path. The general URI path pattern is
`cos://<bucket>.<service>/object(s)`.

```
cos://example-bucket.service1/orders_tiny
```

Trino translates the COS path, and uses the `service1` endpoint and
credentials from `cos-service.properties` to access
`cos://example-bucket.service1/object`.

The Hive Metastore (HMS) does not support the IBM COS filesystem, by default.
The [Stocator library](https://github.com/CODAIT/stocator) is a possible
solution to this problem. Download the [Stocator JAR](https://repo1.maven.org/maven2/com/ibm/stocator/stocator/1.1.4/stocator-1.1.4.jar),
and place it in Hadoop PATH. The [Stocator IBM COS configuration](https://github.com/CODAIT/stocator#reference-stocator-in-the-core-sitexml)
should be placed in `core-site.xml`. For example:

```
<property>
        <name>fs.stocator.scheme.list</name>
        <value>cos</value>
</property>
<property>
        <name>fs.cos.impl</name>
        <value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
</property>
<property>
        <name>fs.stocator.cos.impl</name>
        <value>com.ibm.stocator.fs.cos.COSAPIClient</value>
</property>
<property>
        <name>fs.stocator.cos.scheme</name>
        <value>cos</value>
</property>
<property>
    <name>fs.cos.service1.endpoint</name>
    <value>http://s3.eu-de.cloud-object-storage.appdomain.cloud</value>
</property>
<property>
    <name>fs.cos.service1.access.key</name>
    <value>access-key</value>
</property>
<property>
    <name>fs.cos.service1.secret.key</name>
    <value>secret-key</value>
</property>
```

## Alternative configuration using S3 compatibility

Use the S3 properties for the Hive connector in the catalog file. If only one
IBM Cloud Object Storage endpoint is used, then the configuration can be
simplified:

```
hive.s3.endpoint=http://s3.eu-de.cloud-object-storage.appdomain.cloud
hive.s3.aws-access-key=access-key
hive.s3.aws-secret-key=secret-key
```

Use `s3` protocol instead of `cos` for the table location:

```
s3://example-bucket/object/
```
