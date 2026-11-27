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

import io.trino.testing.containers.Floci;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Hive environment where {@code hdfs://} is overloaded with ViewFS
 * ({@code ViewDistributedFileSystem}) and part of the namespace is mounted to {@code s3a://}.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS, Hive Metastore, and HiveServer2</li>
 *   <li>Floci container providing S3-compatible object storage</li>
 *   <li>Trino container with a {@code core-site.xml} that enables the ViewFS overload scheme for
 *       {@code hdfs://}, and defines a {@code viewfs://cluster} mount table</li>
 *   <li>Hive catalogs, all with the Hadoop file system and the ViewFS {@code core-site.xml}:
 *       <ul>
 *         <li>"hive" with native S3</li>
 *         <li>"hive_extended_settings" with a combination of Hive and S3 settings commonly used together with ViewFS</li>
 *         <li>"hive_recursive_directories" with recursive directory listing</li>
 *         <li>"hive_without_staging" writing directly to the table directory</li>
 *         <li>"hive_without_native_s3" without native S3</li>
 *         <li>"hive_unreachable_s3" with native S3 pointing to an endpoint that refuses connections</li>
 *       </ul>
 *   </li>
 * </ul>
 * <p>
 * The overload scheme mount table maps {@link #S3_MOUNT_PATH} to {@code s3a://<bucket>/<prefix>}, and
 * all other paths fall back to the real HDFS NameNode. The {@code viewfs://cluster} mount table maps
 * {@link #VIEWFS_S3_MOUNT_PATH} to the same {@code s3a://} prefix. The Hive Metastore and HiveServer2
 * only create table directories: they see {@code hdfs://} locations as plain HDFS, and resolve
 * {@code viewfs://cluster} to a plain HDFS directory. Trino resolves both through the mount tables.
 */
public class HiveViewFsEnvironment
        extends ProductTestEnvironment
{
    public static final String S3_MOUNT_PATH = "/tmp/viewfs/s3_data";
    public static final String BUCKET_NAME = "viewfs-bucket";
    public static final String BUCKET_PREFIX = "s3_data";
    public static final String VIEWFS_MOUNT_TABLE = "cluster";
    public static final String VIEWFS_S3_MOUNT_PATH = "/s3_data";
    public static final String CATALOG_WITHOUT_NATIVE_S3 = "hive_without_native_s3";
    public static final String CATALOG_WITH_UNREACHABLE_S3 = "hive_unreachable_s3";
    public static final String CATALOG_WITHOUT_STAGING = "hive_without_staging";
    public static final String CATALOG_WITH_RECURSIVE_DIRECTORIES = "hive_recursive_directories";
    public static final String EXTENDED_SETTINGS_CATALOG = "hive_extended_settings";

    private static final String VIEWFS_CORE_SITE = "/etc/trino/viewfs-core-site.xml";

    private Network network;
    private HadoopContainer hadoop;
    private Floci floci;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (isRunning()) {
            return;
        }

        network = Network.newNetwork();

        floci = new Floci()
                .withNetwork(network)
                .withNetworkAliases("floci");
        floci.start();
        floci.createBucket(BUCKET_NAME);

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                .withCopyToContainer(Transferable.of(metastoreViewFsInitScript(), 0755), "/etc/hadoop-init.d/10-viewfs-mount-table.sh");
        hadoop.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withCommonProperties()
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .put("hive.non-managed-table-writes-enabled", "true")
                        // Native S3 serves the s3a:// mount link through the Hadoop file system bridge
                        .withFlociS3()
                        .build())
                // A combination of Hive settings commonly used together with ViewFS, including S3
                // path-style access, timeouts, part size and SSE-S3
                .withCatalog(EXTENDED_SETTINGS_CATALOG, hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .put("hive.parquet.use-column-names", "true")
                        .put("hive.recursive-directories", "true")
                        .put("hive.metastore.thrift.impersonation.enabled", "false")
                        .put("hive.hdfs.impersonation.enabled", "false")
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .put("hive.ignore-absent-partitions", "true")
                        .put("hive.hive-views.enabled", "true")
                        .put("hive.hive-views.run-as-invoker", "true")
                        .put("hive.metastore-cache-ttl", "6h")
                        .put("hive.metastore-refresh-interval", "2h")
                        .put("hive.metastore-cache-maximum-size", "10000000")
                        .withFlociS3()
                        .put("s3.path-style-access", "true")
                        .put("s3.socket-connect-timeout", "30s")
                        .put("s3.streaming.part-size", "128MB")
                        .put("s3.sse.type", "S3")
                        .build())
                // Reads files in subdirectories of table and partition directories
                .withCatalog(CATALOG_WITH_RECURSIVE_DIRECTORIES, hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withCommonProperties()
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .put("hive.recursive-directories", "true")
                        .withFlociS3()
                        .build())
                // Writes directly to the table directory instead of staging in a temporary directory
                .withCatalog(CATALOG_WITHOUT_STAGING, hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withCommonProperties()
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.temporary-staging-directory-enabled", "false")
                        .withFlociS3()
                        .build())
                .withCatalog(CATALOG_WITHOUT_NATIVE_S3, hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withCommonProperties()
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .build())
                // Same ViewFS mount tables, but native S3 that cannot reach the storage, to verify that
                // catalogs do not share the Hadoop file systems serving the mount links
                .withCatalog(CATALOG_WITH_UNREACHABLE_S3, hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withCommonProperties()
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .withFlociS3()
                        .put("s3.endpoint", "http://127.0.0.1:1")
                        .put("s3.max-error-retries", "1")
                        .put("s3.socket-connect-timeout", "1s")
                        .build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.withCopyToContainer(Transferable.of(viewFsCoreSiteXml()), VIEWFS_CORE_SITE);
        TrinoProductTestContainer.startAndWait(trino);
    }

    // The metastore and HiveServer2 only create table directories, so map viewfs://cluster to plain HDFS for them
    private static String metastoreViewFsInitScript()
    {
        String property = "<property><name>fs.viewfs.mounttable.%s.linkFallback</name><value>hdfs://%s:%d/tmp/viewfs/metastore</value></property>"
                .formatted(VIEWFS_MOUNT_TABLE, HadoopContainer.HOST_NAME, HadoopContainer.HDFS_NAMENODE_PORT);
        return """
               #!/bin/bash
               set -euo pipefail
               for file in /opt/hadoop/etc/hadoop/core-site.xml /etc/hadoop/conf/core-site.xml; do
                   if [ -f "$file" ]; then
                       sed -i 's|</configuration>|%s</configuration>|' "$file"
                   fi
               done
               """.formatted(property);
    }

    private static String viewFsCoreSiteXml()
    {
        // The overload scheme uses the URI host (port ignored) as the mount table name
        String mountTable = HadoopContainer.HOST_NAME;
        return """
               <?xml version="1.0" encoding="UTF-8"?>
               <configuration>
                   <!-- Overload hdfs:// with ViewFS (ViewFileSystemOverloadScheme) -->
                   <property>
                       <name>fs.hdfs.impl</name>
                       <value>org.apache.hadoop.hdfs.ViewDistributedFileSystem</value>
                   </property>
                   <property>
                       <name>fs.viewfs.overload.scheme.target.hdfs.impl</name>
                       <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
                   </property>
                   <property>
                       <name>fs.viewfs.mounttable.%1$s.link.%2$s</name>
                       <value>s3a://%3$s/%4$s</value>
                   </property>
                   <property>
                       <name>fs.viewfs.mounttable.%1$s.linkFallback</name>
                       <value>hdfs://%1$s:%5$d/</value>
                   </property>

                   <!-- viewfs://%10$s mount table, resolved by ViewFileSystem -->
                   <property>
                       <name>fs.viewfs.impl</name>
                       <value>org.apache.hadoop.fs.viewfs.ViewFileSystem</value>
                   </property>
                   <property>
                       <name>fs.viewfs.mounttable.%10$s.link.%11$s</name>
                       <value>s3a://%3$s/%4$s</value>
                   </property>

                   <!-- s3a client settings; fs.viewfs.overload.scheme.target.s3a.impl is deliberately unset -->
                   <property>
                       <name>fs.s3a.endpoint</name>
                       <value>http://floci:%6$d</value>
                   </property>
                   <property>
                       <name>fs.s3a.access.key</name>
                       <value>%7$s</value>
                   </property>
                   <property>
                       <name>fs.s3a.secret.key</name>
                       <value>%8$s</value>
                   </property>
                   <property>
                       <name>fs.s3a.endpoint.region</name>
                       <value>%9$s</value>
                   </property>
                   <property>
                       <name>fs.s3a.path.style.access</name>
                       <value>true</value>
                   </property>
                   <property>
                       <name>fs.s3a.connection.ssl.enabled</name>
                       <value>false</value>
                   </property>
               </configuration>
               """.formatted(
                mountTable,
                S3_MOUNT_PATH,
                BUCKET_NAME,
                BUCKET_PREFIX,
                HadoopContainer.HDFS_NAMENODE_PORT,
                Floci.FLOCI_PORT,
                Floci.FLOCI_ACCESS_KEY,
                Floci.FLOCI_SECRET_KEY,
                Floci.FLOCI_REGION,
                VIEWFS_MOUNT_TABLE,
                VIEWFS_S3_MOUNT_PATH);
    }

    /**
     * Returns the {@code hdfs://} URI that Trino resolves through the ViewFS mount table.
     */
    public String hdfsLocation(String path)
    {
        return "hdfs://%s:%d%s".formatted(HadoopContainer.HOST_NAME, HadoopContainer.HDFS_NAMENODE_PORT, path);
    }

    /**
     * Returns the {@code viewfs://} URI that Trino resolves through the {@code viewfs://cluster} mount table.
     */
    public String viewFsLocation(String path)
    {
        return "viewfs://%s%s".formatted(VIEWFS_MOUNT_TABLE, path);
    }

    /**
     * Executes a statement on HiveServer2. Hive sees {@code hdfs://} locations as plain HDFS and maps
     * {@code viewfs://cluster} to plain HDFS, so it only touches real HDFS.
     */
    public void executeHiveUpdate(@Language("SQL") String sql)
    {
        hadoop.runOnHive(sql);
    }

    /**
     * Uploads an object to the bucket backing the ViewFS {@code s3a://} mount link.
     */
    public void putS3Object(String key, String contents)
    {
        floci.putObject(BUCKET_NAME, contents.getBytes(UTF_8), key);
    }

    public void deleteS3Objects(String prefix)
    {
        floci.deleteObjects(BUCKET_NAME, prefix);
    }

    /**
     * Lists the keys in the bucket backing the ViewFS {@code s3a://} mount link.
     */
    public List<String> listS3Objects(String prefix)
    {
        return floci.listObjects(BUCKET_NAME, prefix);
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, "hive", "hive", "default");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user, "hive", "default");
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (floci != null) {
            floci.close();
            floci = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
