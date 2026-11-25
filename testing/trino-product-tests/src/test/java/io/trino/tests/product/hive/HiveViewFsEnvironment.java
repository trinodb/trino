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
 *   <li>Trino container with the "hive" catalog using the legacy Hadoop file system and a
 *       {@code core-site.xml} that enables the ViewFS overload scheme for {@code hdfs://}</li>
 * </ul>
 * <p>
 * The ViewFS mount table maps {@link #S3_MOUNT_PATH} to {@code s3a://<bucket>/<prefix>}; all other
 * paths fall back to the real HDFS NameNode. The Hive Metastore and HiveServer2 are not
 * ViewFS-aware, so tables created through Hive get a plain HDFS directory, while Trino resolves
 * the same {@code hdfs://} location through the mount table.
 */
public class HiveViewFsEnvironment
        extends ProductTestEnvironment
{
    public static final String S3_MOUNT_PATH = "/tmp/viewfs/s3_data";
    public static final String BUCKET_NAME = "viewfs-bucket";
    public static final String BUCKET_PREFIX = "s3_data";

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
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withCommonProperties()
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.config.resources", "/etc/trino/hdfs-site.xml," + VIEWFS_CORE_SITE)
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.withCopyToContainer(Transferable.of(viewFsCoreSiteXml()), VIEWFS_CORE_SITE);
        TrinoProductTestContainer.startAndWait(trino);
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
                Floci.FLOCI_REGION);
    }

    /**
     * Returns the {@code hdfs://} URI that Trino resolves through the ViewFS mount table.
     */
    public String hdfsLocation(String path)
    {
        return "hdfs://%s:%d%s".formatted(HadoopContainer.HOST_NAME, HadoopContainer.HDFS_NAMENODE_PORT, path);
    }

    /**
     * Executes a statement on HiveServer2. Hive is not ViewFS-aware, so it only touches real HDFS.
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
