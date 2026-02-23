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
package io.trino.testing.containers;

import io.trino.testing.TestingProperties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A Hive 4 Metastore container for product tests.
 * <p>
 * This container runs the Hive 4 standalone Metastore service.
 * Unlike Hive 3 which embeds the metastore in a monolithic container,
 * Hive 4 uses separate containers for Metastore and HiveServer2.
 * <p>
 * Key features:
 * <ul>
 *   <li>Hive Metastore on port 9083</li>
 *   <li>Configured for S3 (MinIO) storage via hive-site.xml</li>
 *   <li>Uses SERVICE_NAME=metastore to run as standalone metastore</li>
 * </ul>
 *
 * @see Hive4HiveServerContainer
 */
public class Hive4MetastoreContainer
        extends GenericContainer<Hive4MetastoreContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/hive4.0-hive";

    public static final String HOST_NAME = "metastore";
    public static final int HIVE_METASTORE_PORT = 9083;

    private static final String DEFAULT_WAREHOUSE_DIR = "/opt/hive/data/warehouse";

    // Template placeholders: warehouseDir, accessKey, secretKey, s3Host, s3Port
    private static final String HIVE_SITE_XML_TEMPLATE = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>
                <property>
                    <name>hive.server2.enable.doAs</name>
                    <value>false</value>
                </property>
                <property>
                    <name>hive.tez.exec.inplace.progress</name>
                    <value>false</value>
                </property>
                <property>
                    <name>hive.exec.scratchdir</name>
                    <value>/opt/hive/scratch_dir</value>
                </property>
                <property>
                    <name>hive.user.install.directory</name>
                    <value>/opt/hive/install_dir</value>
                </property>
                <property>
                    <name>tez.runtime.optimize.local.fetch</name>
                    <value>true</value>
                </property>
                <property>
                    <name>hive.exec.submit.local.task.via.child</name>
                    <value>false</value>
                </property>
                <property>
                    <name>mapreduce.framework.name</name>
                    <value>local</value>
                </property>
                <property>
                    <name>hive.metastore.warehouse.dir</name>
                    <value>%s</value>
                </property>
                <property>
                    <name>metastore.metastore.event.db.notification.api.auth</name>
                    <value>false</value>
                </property>
                <!-- Required to get past 'Cannot set role admin' error -->
                <property>
                    <name>hive.users.in.admin.role</name>
                    <value>hive</value>
                </property>
                <!-- S3 file system properties -->
                <property>
                    <name>fs.s3a.access.key</name>
                    <value>%s</value>
                </property>
                <property>
                    <name>fs.s3a.secret.key</name>
                    <value>%s</value>
                </property>
                <property>
                    <name>fs.s3a.endpoint</name>
                    <value>http://%s:%d</value>
                </property>
                <property>
                    <name>fs.s3a.path.style.access</name>
                    <value>true</value>
                </property>
                <property>
                    <name>fs.s3.impl</name>
                    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
                </property>
            </configuration>
            """;

    private String warehouseDir = DEFAULT_WAREHOUSE_DIR;

    public Hive4MetastoreContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
    }

    public Hive4MetastoreContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(HIVE_METASTORE_PORT);
        withEnv("SERVICE_NAME", "metastore");
        // Default S3 configuration pointing to MinIO
        withCopyToContainer(
                Transferable.of(getHiveSiteXml(
                        DEFAULT_WAREHOUSE_DIR,
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD,
                        Minio.DEFAULT_HOST_NAME,
                        Minio.MINIO_API_PORT)),
                "/opt/hive/conf/hive-site.xml");
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    /**
     * Sets the warehouse directory for the Hive Metastore.
     * This can be a local path or an S3A path (e.g., "s3a://bucket-name/warehouse").
     *
     * @param warehouseDir the warehouse directory path
     * @return this container for chaining
     */
    public Hive4MetastoreContainer withWarehouseDir(String warehouseDir)
    {
        this.warehouseDir = warehouseDir;
        return this;
    }

    /**
     * Configures S3 settings for the Hive Metastore.
     *
     * @param accessKey S3 access key
     * @param secretKey S3 secret key
     * @param s3Host S3 endpoint host
     * @param s3Port S3 endpoint port
     * @return this container for chaining
     */
    public Hive4MetastoreContainer withS3Config(String accessKey, String secretKey, String s3Host, int s3Port)
    {
        withCopyToContainer(
                Transferable.of(getHiveSiteXml(warehouseDir, accessKey, secretKey, s3Host, s3Port)),
                "/opt/hive/conf/hive-site.xml");
        return this;
    }

    @Override
    public void start()
    {
        // Ensure the hive-site.xml is generated with the current warehouseDir before starting
        withCopyToContainer(
                Transferable.of(getHiveSiteXml(
                        warehouseDir,
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD,
                        Minio.DEFAULT_HOST_NAME,
                        Minio.MINIO_API_PORT)),
                "/opt/hive/conf/hive-site.xml");
        super.start();
    }

    private static String getHiveSiteXml(String warehouseDir, String accessKey, String secretKey, String s3Host, int s3Port)
    {
        return HIVE_SITE_XML_TEMPLATE.formatted(warehouseDir, accessKey, secretKey, s3Host, s3Port);
    }

    /**
     * Returns the configured warehouse directory path.
     *
     * @return the warehouse directory path
     */
    public String getWarehouseDir()
    {
        return warehouseDir;
    }

    /**
     * Returns the Thrift URI for connecting to this Metastore from the host.
     */
    public String getHiveMetastoreUri()
    {
        return "thrift://" + getHost() + ":" + getMappedPort(HIVE_METASTORE_PORT);
    }

    /**
     * Returns the internal Thrift URI for connecting from other containers on the same network.
     */
    public String getInternalHiveMetastoreUri()
    {
        return getInternalMetastoreUri();
    }

    /**
     * Returns the internal Thrift URI for connecting from other containers on the same network.
     * Static version that can be used before container instance is created.
     */
    public static String getInternalMetastoreUri()
    {
        return "thrift://" + HOST_NAME + ":" + HIVE_METASTORE_PORT;
    }
}
