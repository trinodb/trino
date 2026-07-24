/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.hive;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.Minio;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class HiveCatalogPropertiesBuilder
{
    private static final Map<String, String> HIVE_COMMON_PROPERTIES = Map.of(
            "hive.metastore-cache-ttl", "0s",
            "hive.hive-views.enabled", "true",
            "hive.parquet.time-zone", "UTC",
            "hive.rcfile.time-zone", "UTC");

    private static final Map<String, String> HIVE_PARTITION_PROCEDURE_PROPERTIES = Map.of(
            "hive.allow-register-partition-procedure", "true",
            "hive.max-partitions-per-scan", "100",
            "hive.max-partitions-for-eager-load", "100");

    private static final Map<String, String> HADOOP_FILESYSTEM_PROPERTIES = Map.of(
            "fs.hadoop.enabled", "true",
            "hive.config.resources", "/etc/trino/hdfs-site.xml");

    private static final Map<String, String> DISABLE_HADOOP_FILESYSTEM_PROPERTY = Map.of(
            "fs.hadoop.enabled", "false");

    private static final Map<String, String> MINIO_S3_PROPERTIES = Map.of(
            "fs.native-s3.enabled", "true",
            "s3.endpoint", "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
            "s3.aws-access-key", Minio.MINIO_ROOT_USER,
            "s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD,
            "s3.path-style-access", "true",
            "s3.region", Minio.MINIO_REGION);

    private final Map<String, String> properties = new LinkedHashMap<>();

    private HiveCatalogPropertiesBuilder() {}

    public static String hadoopMetastoreUri()
    {
        return "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT;
    }

    public static HiveCatalogPropertiesBuilder hiveCatalog(String metastoreUri)
    {
        return new HiveCatalogPropertiesBuilder()
                .put("connector.name", "hive")
                .put("hive.metastore.uri", metastoreUri);
    }

    public static HiveCatalogPropertiesBuilder hudiCatalog(String metastoreUri)
    {
        return new HiveCatalogPropertiesBuilder()
                .put("connector.name", "hudi")
                .put("hive.metastore.uri", metastoreUri);
    }

    public HiveCatalogPropertiesBuilder withHadoopFileSystem()
    {
        return putAll(HADOOP_FILESYSTEM_PROPERTIES);
    }

    public HiveCatalogPropertiesBuilder withHadoopFileSystemDisabled()
    {
        return putAll(DISABLE_HADOOP_FILESYSTEM_PROPERTY);
    }

    public HiveCatalogPropertiesBuilder withMinioS3()
    {
        return putAll(MINIO_S3_PROPERTIES);
    }

    public HiveCatalogPropertiesBuilder withCommonProperties()
    {
        return putAll(HIVE_COMMON_PROPERTIES);
    }

    public HiveCatalogPropertiesBuilder withPartitionProcedures()
    {
        return putAll(HIVE_PARTITION_PROCEDURE_PROPERTIES);
    }

    public HiveCatalogPropertiesBuilder put(String key, String value)
    {
        properties.put(key, value);
        return this;
    }

    public HiveCatalogPropertiesBuilder putAll(Map<String, String> additionalProperties)
    {
        properties.putAll(additionalProperties);
        return this;
    }

    public Map<String, String> build()
    {
        return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }
}
