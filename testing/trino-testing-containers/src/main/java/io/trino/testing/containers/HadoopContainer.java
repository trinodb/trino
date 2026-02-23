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

import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A reusable Hadoop container for product tests.
 * <p>
 * This container provides:
 * <ul>
 *   <li>Hive Metastore on port 9083</li>
 *   <li>HiveServer2 on port 10000</li>
 *   <li>HDFS NameNode on port 9000</li>
 *   <li>WebHDFS on port 9870</li>
 * </ul>
 * <p>
 * The container uses the {@code ghcr.io/trinodb/testing/hive3.1} image.
 * For JUnit migration fidelity, migrated Hive lanes should use hive3.1 image naming.
 */
public class HadoopContainer
        extends GenericContainer<HadoopContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/hive3.1";
    private static final String KERBERIZED_IMAGE = "ghcr.io/trinodb/testing/hive3.1-kerberos";

    public static final String HOST_NAME = "hadoop-master";

    public static final int HIVE_METASTORE_PORT = 9083;
    public static final int HIVESERVER2_PORT = 10000;
    public static final int HDFS_NAMENODE_PORT = 9000;
    public static final int WEBHDFS_PORT = 9870;

    private final String hostName;
    private final boolean kerberizedImage;
    private boolean lzoCodecEnabled;
    private boolean trinoProxyUserEnabled;
    private S3Config s3Config;

    /**
     * Configuration for S3-compatible storage (like Minio).
     */
    public record S3Config(String endpoint, String accessKey, String secretKey) {}

    public HadoopContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion(), HOST_NAME);
    }

    public HadoopContainer(String imageName)
    {
        this(imageName, HOST_NAME);
    }

    public HadoopContainer(String imageName, String hostName)
    {
        super(DockerImageName.parse(imageName));
        this.hostName = requireNonNull(hostName, "hostName is null");
        this.kerberizedImage = imageName.contains("-kerberized") || imageName.contains("-kerberos");
        withExposedPorts(HIVE_METASTORE_PORT, HIVESERVER2_PORT, HDFS_NAMENODE_PORT, WEBHDFS_PORT);
        // Set the container hostname to match the network alias
        // This is required for Kerberos authentication where principal hostnames must match
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostName));
        withEnv("TZ", "UTC");
        // Non-kerberized lanes require simple-auth Hadoop/Hive configs.
        // Kerberized image lanes keep image-native configs.
        if (shouldOverrideHdfsSiteXml()) {
            withCopyToContainer(
                    Transferable.of(getHdfsSiteXml()),
                    "/opt/hadoop/etc/hadoop/hdfs-site.xml");
        }
        // Non-kerberized lanes require plain HMS thrift and HS2 auth.
        if (shouldOverrideHiveSiteXml()) {
            withCopyToContainer(
                    Transferable.of(getHiveSiteXml()),
                    "/opt/hive/conf/hive-site.xml");
        }
        // Always run via a startup wrapper that applies /etc/hadoop-init.d scripts before supervisord.
        // Many environments rely on this hook (Kerberos, Azure/GCS, ACID) to patch runtime config.
        withCopyToContainer(
                Transferable.of(HADOOP_RUN_SCRIPT, 0755),
                "/usr/local/hadoop-run.sh");
        withCommand("/usr/local/hadoop-run.sh");
        // Note: core-site.xml is copied in containerIsStarting() to allow for LZO codec configuration
        // Wait for socks-proxy to enter RUNNING state - this is the last service started by supervisord
        // Note: Don't use Wait.forListeningPort() as HDFS port 9000 binds to container IP, not 0.0.0.0
        waitingFor(Wait.forLogMessage(".*success: socks-proxy entered RUNNING state.*", 1)
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    @Override
    protected void configure()
    {
        super.configure();
        // Non-kerberized lanes require simple-auth core-site wiring.
        // Kerberized lanes keep image defaults unless explicit feature wiring is requested.
        if (shouldOverrideCoreSiteXml()) {
            // This is done in configure() so it happens after withLzoCodec() but before container creation.
            // For S3 config, the init script (S3_JARS_INIT_SCRIPT) copies this to /etc/hadoop/conf/
            // which is where HMS looks for S3 config during table location validation.
            withCopyToContainer(
                    Transferable.of(getCoreSiteXml(lzoCodecEnabled)),
                    "/opt/hadoop/etc/hadoop/core-site.xml");
        }
    }

    /**
     * Creates a HadoopContainer with a custom hostname.
     * <p>
     * Use this factory method when you need multiple Hadoop containers
     * in the same network with different hostnames (e.g., for dual-cluster environments).
     *
     * @param hostName the hostname to use for this container
     * @return a new HadoopContainer with the specified hostname
     */
    public static HadoopContainer withHostName(String hostName)
    {
        return new HadoopContainer(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion(), hostName);
    }

    /**
     * Creates a Kerberos-preconfigured HadoopContainer.
     * <p>
     * This uses the hive3.1 Kerberos-preconfigured image variant.
     */
    public static HadoopContainer kerberized()
    {
        return new HadoopContainer(KERBERIZED_IMAGE + ":" + TestingProperties.getDockerImagesVersion(), HOST_NAME);
    }

    /**
     * Creates a Kerberos-preconfigured HadoopContainer with a custom hostname.
     */
    public static HadoopContainer kerberizedWithHostName(String hostName)
    {
        return new HadoopContainer(KERBERIZED_IMAGE + ":" + TestingProperties.getDockerImagesVersion(), hostName);
    }

    /**
     * Returns the hostname configured for this container.
     */
    public String getHostName()
    {
        return hostName;
    }

    private boolean shouldOverrideHdfsSiteXml()
    {
        return !kerberizedImage;
    }

    private boolean shouldOverrideCoreSiteXml()
    {
        return !kerberizedImage || lzoCodecEnabled || trinoProxyUserEnabled || s3Config != null;
    }

    private boolean shouldOverrideHiveSiteXml()
    {
        return !kerberizedImage && s3Config == null;
    }

    // HDFS configuration for reliable container-to-container communication
    private String getHdfsSiteXml()
    {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <configuration>
                    <!-- Disable permissions for test simplicity -->
                    <property>
                        <name>dfs.permissions.enabled</name>
                        <value>false</value>
                    </property>

                    <!-- Use hostnames instead of IPs for DataNode communication -->
                    <property>
                        <name>dfs.client.use.datanode.hostname</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>dfs.datanode.use.datanode.hostname</name>
                        <value>true</value>
                    </property>

                    <!-- Explicitly set the DataNode hostname to the network alias -->
                    <!-- Without this, the DataNode advertises its container ID as hostname -->
                    <property>
                        <name>dfs.datanode.hostname</name>
                        <value>%s</value>
                    </property>

                    <!-- Increased timeouts for container environments -->
                    <property>
                        <name>dfs.client.socket-timeout</name>
                        <value>180000</value>
                    </property>
                    <property>
                        <name>dfs.datanode.socket.write.timeout</name>
                        <value>600000</value>
                    </property>

                    <!-- Single replication for test environments -->
                    <property>
                        <name>dfs.replication</name>
                        <value>1</value>
                    </property>

                    <!-- Disable short-circuit reads (simplifies container setup) -->
                    <property>
                        <name>dfs.client.read.shortcircuit</name>
                        <value>false</value>
                    </property>
                </configuration>
                """.formatted(hostName);
    }

    // Core-site configuration with HDFS default and proxy user settings
    private String getCoreSiteXml()
    {
        return getCoreSiteXml(false);
    }

    // Core-site configuration with optional LZO codec registration
    private String getCoreSiteXml(boolean includeLzoCodecs)
    {
        String lzoCodecConfig = includeLzoCodecs ? """

                    <!-- LZO compression codecs -->
                    <property>
                        <name>io.compression.codecs</name>
                        <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec</value>
                    </property>
                    <property>
                        <name>io.compression.codec.lzo.class</name>
                        <value>com.hadoop.compression.lzo.LzoCodec</value>
                    </property>
                """ : "";

        String s3ConfigXml = s3Config != null ? """

                    <!-- S3/Minio configuration for Hive Metastore -->
                    <property>
                        <name>fs.s3a.endpoint</name>
                        <value>%s</value>
                    </property>
                    <property>
                        <name>fs.s3a.access.key</name>
                        <value>%s</value>
                    </property>
                    <property>
                        <name>fs.s3a.secret.key</name>
                        <value>%s</value>
                    </property>
                    <property>
                        <name>fs.s3a.path.style.access</name>
                        <value>true</value>
                    </property>
                    <!-- Use simple credentials provider to ensure access/secret keys are used -->
                    <property>
                        <name>fs.s3a.aws.credentials.provider</name>
                        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
                    </property>
                    <!-- Disable SSL for Minio -->
                    <property>
                        <name>fs.s3a.connection.ssl.enabled</name>
                        <value>false</value>
                    </property>
                    <!-- Map s3:// and s3n:// schemes to S3AFileSystem -->
                    <property>
                        <name>fs.s3.impl</name>
                        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
                    </property>
                    <property>
                        <name>fs.s3n.impl</name>
                        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
                    </property>
                """.formatted(s3Config.endpoint(), s3Config.accessKey(), s3Config.secretKey()) : "";

        String trinoProxyUserConfig = trinoProxyUserEnabled ? """

                    <!-- Allow trino user to impersonate any user from any host -->
                    <property>
                        <name>hadoop.proxyuser.trino.hosts</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.trino.groups</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.trino.users</name>
                        <value>*</value>
                    </property>
                """ : "";

        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <configuration>
                    <!-- Default filesystem - use hostname for cross-container access -->
                    <property>
                        <name>fs.defaultFS</name>
                        <value>hdfs://%s:%d</value>
                    </property>

                    <!-- Allow hive user to impersonate any user from any host -->
                    <property>
                        <name>hadoop.proxyuser.hive.hosts</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.hive.groups</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.hive.users</name>
                        <value>*</value>
                    </property>

                    <!-- Allow hdfs service user to impersonate for filesystem operations -->
                    <property>
                        <name>hadoop.proxyuser.hdfs.hosts</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.hdfs.groups</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.hdfs.users</name>
                        <value>*</value>
                    </property>

                    <!-- HiveServer2 runs as root in this image and needs doAs proxy permissions -->
                    <property>
                        <name>hadoop.proxyuser.root.hosts</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.root.groups</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.proxyuser.root.users</name>
                        <value>*</value>
                    </property>%s%s%s
                </configuration>
                """.formatted(hostName, HDFS_NAMENODE_PORT, lzoCodecConfig, s3ConfigXml, trinoProxyUserConfig);
    }

    /**
     * Returns the Thrift URI for the Hive Metastore.
     * <p>
     * Note: This returns the internal network address when used with network aliases.
     * Use this method only when containers communicate within the same Docker network.
     */
    public String getHiveMetastoreUri()
    {
        return "thrift://" + hostName + ":" + HIVE_METASTORE_PORT;
    }

    /**
     * Returns the externally accessible Hive Metastore URI for connecting from the host.
     */
    public String getExternalHiveMetastoreUri()
    {
        return "thrift://" + getHost() + ":" + getMappedPort(HIVE_METASTORE_PORT);
    }

    /**
     * Executes a Hive query using beeline.
     */
    public String runOnHive(String query)
    {
        try {
            ExecResult result = execInContainer("beeline", "-u", "jdbc:hive2://localhost:10000/default", "-n", "hive", "-e", query);
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Hive query failed: " + result.getStderr());
            }
            return result.getStdout();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute Hive query", e);
        }
    }

    /**
     * Returns the WebHDFS URL for connecting from the host.
     */
    public String getWebHdfsUrl()
    {
        return "http://" + getHost() + ":" + getMappedPort(WEBHDFS_PORT);
    }

    /**
     * Returns a hostname to IP address mapping for this container.
     * <p>
     * This mapping allows the host machine to resolve Docker-internal hostnames
     * (like "hadoop-master") by mapping them to the container's IP address.
     * This is necessary for WebHDFS operations where the NameNode returns redirects
     * to DataNodes using internal hostnames.
     *
     * @return a map from hostname to IP address
     */
    public Map<String, InetAddress> getHostMapping()
    {
        String containerIp = getContainerInfo()
                .getNetworkSettings()
                .getNetworks()
                .values()
                .iterator()
                .next()
                .getIpAddress();
        try {
            return Map.of(hostName, InetAddress.getByName(containerIp));
        }
        catch (UnknownHostException e) {
            throw new RuntimeException("Failed to resolve container IP: " + containerIp, e);
        }
    }

    /**
     * Creates an HdfsClient for interacting with HDFS via WebHDFS.
     * <p>
     * The client uses custom DNS resolution to handle redirects to DataNodes
     * using Docker-internal hostnames.
     *
     * @param username the HDFS username to use
     * @return a new HdfsClient
     */
    public HdfsClient createHdfsClient(String username)
    {
        return new HdfsClient(URI.create(getWebHdfsUrl()), username, getHostMapping());
    }

    /**
     * Creates an HdfsClient using the default "hive" user.
     * <p>
     * The client uses custom DNS resolution to handle redirects to DataNodes
     * using Docker-internal hostnames.
     *
     * @return a new HdfsClient
     */
    public HdfsClient createHdfsClient()
    {
        return createHdfsClient("hive");
    }

    /**
     * Returns HDFS client configuration XML for use by other containers.
     * <p>
     * This XML contains the client-side HDFS settings needed for reliable
     * container-to-container communication, including:
     * <ul>
     *   <li>Default filesystem URI pointing to this container's NameNode</li>
     *   <li>Use hostnames instead of IPs for DataNode communication</li>
     *   <li>Increased timeouts for container environments</li>
     *   <li>Single replication for test environments</li>
     *   <li>Short-circuit reads disabled for container simplicity</li>
     * </ul>
     *
     * @return HDFS configuration XML suitable for Trino or other HDFS clients
     */
    public String getHdfsClientSiteXml()
    {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <configuration>
                    <!-- Default filesystem - HDFS on %1$s -->
                    <property>
                        <name>fs.defaultFS</name>
                        <value>hdfs://%1$s:%2$d</value>
                    </property>

                    <!-- Use hostnames instead of IPs for DataNode communication -->
                    <property>
                        <name>dfs.client.use.datanode.hostname</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>dfs.datanode.use.datanode.hostname</name>
                        <value>true</value>
                    </property>

                    <!-- Increased timeouts for container environments -->
                    <property>
                        <name>dfs.client.socket-timeout</name>
                        <value>180000</value>
                    </property>
                    <property>
                        <name>dfs.datanode.socket.write.timeout</name>
                        <value>600000</value>
                    </property>

                    <!-- Single replication for test environments -->
                    <property>
                        <name>dfs.replication</name>
                        <value>1</value>
                    </property>

                    <!-- Disable short-circuit reads (simplifies container setup) -->
                    <property>
                        <name>dfs.client.read.shortcircuit</name>
                        <value>false</value>
                    </property>
                </configuration>
                """.formatted(hostName, HDFS_NAMENODE_PORT);
    }

    /**
     * Returns the default warehouse directory path in HDFS.
     */
    public String getWarehouseDirectory()
    {
        return "/user/hive/warehouse";
    }

    // Startup script that runs init.d scripts then starts supervisord
    // This is the same pattern used by the product tests launcher
    private static final String HADOOP_RUN_SCRIPT = """
            #!/usr/bin/env bash
            set -euo pipefail

            HADOOP_INIT_D=${HADOOP_INIT_D:-/etc/hadoop-init.d/}

            echo "Applying hadoop init.d scripts from ${HADOOP_INIT_D}"
            if test -d "${HADOOP_INIT_D}"; then
                for init_script in "${HADOOP_INIT_D}"*; do
                    if [ -x "$init_script" ]; then
                        echo "Running $init_script"
                        "$init_script"
                    fi
                done
            fi

            trap exit INT

            echo "Running services with supervisord"
            exec supervisord -c /etc/supervisord.conf
            """;

    // Init script that copies S3 jars to Hive classpath and sets up Hadoop config
    private static final String S3_JARS_INIT_SCRIPT = """
            #!/bin/bash
            set -e
            echo "Copying hadoop-aws jars to Hive classpath..."
            cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar /opt/hive/lib/
            cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar /opt/hive/lib/
            echo "S3 jars copied successfully"

            # Create /etc/hadoop/conf directory and copy core-site.xml for HMS
            # HMS uses this location for S3 location validation during table creation
            echo "Setting up Hadoop config for HMS..."
            mkdir -p /etc/hadoop/conf
            cp /opt/hadoop/etc/hadoop/core-site.xml /etc/hadoop/conf/
            echo "Hadoop config setup complete"
            """;

    // Generates a baseline non-kerberos hive-site.xml for the default Hive 3.1 lane
    private String getHiveSiteXml()
    {
        return """
                <?xml version="1.0"?>
                <configuration>
                    <property>
                        <name>hive.metastore.uris</name>
                        <value>thrift://localhost:9083</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionURL</name>
                        <value>jdbc:mysql://localhost:3306/metastore?useSSL=false</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionDriverName</name>
                        <value>com.mysql.cj.jdbc.Driver</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionUserName</name>
                        <value>root</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionPassword</name>
                        <value>root</value>
                    </property>
                    <property>
                        <name>hive.metastore.connect.retries</name>
                        <value>15</value>
                    </property>
                    <property>
                        <name>metastore.storage.schema.reader.impl</name>
                        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
                    </property>
                    <property>
                        <name>hive.support.concurrency</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>hive.txn.manager</name>
                        <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
                    </property>
                    <property>
                        <name>hive.compactor.initiator.on</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>hive.compactor.worker.threads</name>
                        <value>1</value>
                    </property>
                    <property>
                        <name>hive.metastore.disallow.incompatible.col.type.changes</name>
                        <value>false</value>
                    </property>
                    <property>
                        <name>hive.server2.authentication</name>
                        <value>NONE</value>
                    </property>
                    <property>
                        <name>hive.server2.enable.doAs</name>
                        <value>false</value>
                    </property>
                    <property>
                        <name>hive.metastore.sasl.enabled</name>
                        <value>false</value>
                    </property>
                    <property>
                        <name>hive.metastore.event.db.notification.api.auth</name>
                        <value>false</value>
                    </property>
                    <property>
                        <name>hive.security.authorization.manager</name>
                        <value>org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory</value>
                    </property>
                    <property>
                        <name>hive.security.authorization.task.factory</name>
                        <value>org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl</value>
                    </property>
                </configuration>
                """;
    }

    // Generates a complete hive-site.xml with S3 configuration
    private String getHiveSiteXml(S3Config s3)
    {
        return """
                <?xml version="1.0"?>
                <configuration>
                    <property>
                        <name>hive.metastore.uris</name>
                        <value>thrift://localhost:9083</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionURL</name>
                        <value>jdbc:mysql://localhost:3306/metastore?useSSL=false</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionDriverName</name>
                        <value>com.mysql.cj.jdbc.Driver</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionUserName</name>
                        <value>root</value>
                    </property>
                    <property>
                        <name>javax.jdo.option.ConnectionPassword</name>
                        <value>root</value>
                    </property>
                    <property>
                        <name>hive.metastore.connect.retries</name>
                        <value>15</value>
                    </property>
                    <property>
                        <name>metastore.storage.schema.reader.impl</name>
                        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
                    </property>
                    <property>
                        <name>hive.support.concurrency</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>hive.txn.manager</name>
                        <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
                    </property>
                    <property>
                        <name>hive.compactor.initiator.on</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>hive.compactor.worker.threads</name>
                        <value>1</value>
                    </property>
                    <property>
                        <name>hive.metastore.disallow.incompatible.col.type.changes</name>
                        <value>false</value>
                    </property>
                    <!-- S3 configuration for Minio -->
                    <property>
                        <name>fs.s3a.endpoint</name>
                        <value>%s</value>
                    </property>
                    <property>
                        <name>fs.s3a.access.key</name>
                        <value>%s</value>
                    </property>
                    <property>
                        <name>fs.s3a.secret.key</name>
                        <value>%s</value>
                    </property>
                    <property>
                        <name>fs.s3a.path.style.access</name>
                        <value>true</value>
                    </property>
                    <property>
                        <name>fs.s3a.connection.ssl.enabled</name>
                        <value>false</value>
                    </property>
                    <property>
                        <name>fs.s3a.aws.credentials.provider</name>
                        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
                    </property>
                    <property>
                        <name>fs.s3.impl</name>
                        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
                    </property>
                    <property>
                        <name>fs.s3n.impl</name>
                        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
                    </property>
                </configuration>
                """.formatted(s3.endpoint(), s3.accessKey(), s3.secretKey());
    }

    /**
     * Configures the container for S3-compatible storage (like Minio).
     * <p>
     * This adds the necessary Hadoop configuration to allow the Hive Metastore
     * to understand s3:// and s3a:// URLs for table locations.
     * <p>
     * This also ensures the hadoop-aws jar is on the classpath by adding it
     * to the Hive auxlib directory.
     * <p>
     * This method must be called before {@link #start()}.
     */
    public HadoopContainer withS3Config(String endpoint, String accessKey, String secretKey)
    {
        this.s3Config = new S3Config(endpoint, accessKey, secretKey);
        // Add environment variables as fallback for S3 credentials
        withEnv("AWS_ACCESS_KEY_ID", accessKey);
        withEnv("AWS_SECRET_ACCESS_KEY", secretKey);
        // Copy an init script that copies hadoop-aws jars to Hive's classpath
        withCopyToContainer(
                Transferable.of(S3_JARS_INIT_SCRIPT, 0755),
                "/etc/hadoop-init.d/00-copy-s3-jars.sh");
        // Copy a complete hive-site.xml with S3 configuration
        withCopyToContainer(
                Transferable.of(getHiveSiteXml(s3Config)),
                "/opt/hive/conf/hive-site.xml");
        return this;
    }

    /**
     * Enables HDFS proxy-user permissions for the Trino service user.
     * <p>
     * This is required for environments where Trino performs HDFS impersonation
     * (for example {@code hive.hdfs.impersonation.enabled=true}).
     */
    public HadoopContainer withTrinoProxyUser()
    {
        this.trinoProxyUserEnabled = true;
        return this;
    }

    /**
     * Configures the container to include the pure Java LZO compression codec.
     * <p>
     * This downloads lzo-hadoop and lzo-core JARs from Maven Central and
     * bind mounts them into the Hive lib directory, enabling LZO/LZOP compression
     * support without any native library dependencies.
     * <p>
     * This method must be called before {@link #start()}.
     *
     * @return this container for method chaining
     * @throws RuntimeException if downloading the JARs fails
     */
    public HadoopContainer withLzoCodec()
    {
        try {
            String lzoHadoopUrl = "https://repo1.maven.org/maven2/org/anarres/lzo/lzo-hadoop/1.0.6/lzo-hadoop-1.0.6.jar";
            String lzoCoreUrl = "https://repo1.maven.org/maven2/org/anarres/lzo/lzo-core/1.0.6/lzo-core-1.0.6.jar";

            byte[] lzoHadoopJar = downloadUrl(lzoHadoopUrl);
            byte[] lzoCoreJar = downloadUrl(lzoCoreUrl);

            // Verify downloads succeeded
            if (lzoHadoopJar.length < 1000) {
                throw new RuntimeException("lzo-hadoop JAR download appears to have failed, size=" + lzoHadoopJar.length);
            }
            if (lzoCoreJar.length < 1000) {
                throw new RuntimeException("lzo-core JAR download appears to have failed, size=" + lzoCoreJar.length);
            }

            // Copy to Hive lib directory
            withCopyToContainer(Transferable.of(lzoHadoopJar), "/opt/hive/lib/lzo-hadoop-1.0.6.jar");
            withCopyToContainer(Transferable.of(lzoCoreJar), "/opt/hive/lib/lzo-core-1.0.6.jar");

            // Also copy to Hadoop lib directory for MapReduce jobs
            withCopyToContainer(Transferable.of(lzoHadoopJar), "/opt/hadoop/share/hadoop/common/lib/lzo-hadoop-1.0.6.jar");
            withCopyToContainer(Transferable.of(lzoCoreJar), "/opt/hadoop/share/hadoop/common/lib/lzo-core-1.0.6.jar");

            // Enable LZO codec registration in core-site.xml (applied in containerIsStarting)
            this.lzoCodecEnabled = true;

            return this;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to configure LZO codec", e);
        }
    }

    private static byte[] downloadUrl(String url)
            throws Exception
    {
        try (InputStream in = URI.create(url).toURL().openStream()) {
            return in.readAllBytes();
        }
    }
}
