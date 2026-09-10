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

import io.airlift.log.Logger;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.KerberosContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Kerberos-enabled Hive product test environment.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Standalone KDC container for Kerberos authentication</li>
 *   <li>Hadoop container (HDFS, Hive Metastore) configured for Kerberos</li>
 *   <li>Trino container with Kerberos-enabled Hive connector</li>
 * </ul>
 * <p>
 * <b>Implementation:</b>
 * <p>
 * The Hadoop image provides the standard Kerberos client tooling and Hadoop/Hive configuration.
 * It also has a built-in init hook mechanism where scripts in {@code /etc/hadoop-init.d/}
 * are executed before supervisord starts. This environment injects a script that:
 * <ul>
 *   <li>Applies environment-specific Hadoop/Hive configuration overrides</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>
 * ┌────────────────────────────────────────────────────────────────┐
 * │                      Docker Network                            │
 * │                                                                │
 * │  ┌─────────────────┐  ┌─────────────────────────────────────┐  │
 * │  │ kdc container   │  │ hive3.1                            │  │
 * │  │                 │  │                                     │  │
 * │  │ KDC on :88      │◄─│ /etc/hadoop-init.d/                 │  │
 * │  │ create_principal│  │   └─ 00-kerberos-init.sh ← INJECTED │  │
 * │  └─────────────────┘  │                                     │  │
 * │                       │ Runs BEFORE supervisord:            │  │
 * │  ┌─────────────────┐  │   • Writes krb5.conf                │  │
 * │  │ trino           │  │   • Copies keytabs                  │  │
 * │  │                 │  │   • Modifies *-site.xml             │  │
 * │  │ Hive catalog    │  │   • Then services start kerberized  │  │
 * │  │ with Kerberos   │  └─────────────────────────────────────┘  │
 * │  └─────────────────┘                                           │
 * └────────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * <b>Principals created:</b>
 * <ul>
 *   <li>hdfs/hadoop-master@TRINO.TEST - for HDFS services</li>
 *   <li>hive/hadoop-master@TRINO.TEST - for Hive Metastore</li>
 *   <li>HTTP/hadoop-master@TRINO.TEST - for WebHDFS/HTTP SPNEGO</li>
 *   <li>mapred/hadoop-master@TRINO.TEST - for MapReduce services</li>
 *   <li>yarn/hadoop-master@TRINO.TEST - for YARN services</li>
 *   <li>trino/trino-master@TRINO.TEST - for Trino service</li>
 * </ul>
 * <p>
 * <b>Extensibility:</b>
 * <p>
 * Subclasses can customize behavior by overriding extension point methods:
 * <ul>
 *   <li>{@link #getAdditionalPrincipals()} - add extra Kerberos principals</li>
 *   <li>{@link #getCoreSiteProperties()} - add properties to core-site.xml</li>
 *   <li>{@link #getHdfsSiteProperties()} - add properties to hdfs-site.xml</li>
 *   <li>{@link #getHdfsClientSiteProperties()} - add properties to HDFS client config</li>
 *   <li>{@link #getAdditionalCatalogProperties()} - add Trino catalog properties</li>
 *   <li>{@link #createAdditionalContainers()} - start additional containers</li>
 *   <li>{@link #afterHadoopStart()} - execute after Hadoop starts</li>
 *   <li>{@link #writeAdditionalKeytabs(Path)} - write additional keytab files</li>
 * </ul>
 */
public class HiveKerberosEnvironment
        extends ProductTestEnvironment
{
    private static final Logger log = Logger.get(HiveKerberosEnvironment.class);
    private static final Logger hadoopLog = Logger.get("Hadoop");
    private static final Duration HIVE_METASTORE_STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration HIVE_METASTORE_STARTUP_POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int HIVE_METASTORE_STABLE_SUCCESS_POLL_COUNT = 3;

    // Principal names (without realm)
    protected static final String HDFS_PRINCIPAL = "hdfs/hadoop-master";
    protected static final String HIVE_PRINCIPAL = "hive/hadoop-master";
    protected static final String HTTP_PRINCIPAL = "HTTP/hadoop-master";
    protected static final String MAPRED_PRINCIPAL = "mapred/hadoop-master";
    protected static final String YARN_PRINCIPAL = "yarn/hadoop-master";
    protected static final String TRINO_PRINCIPAL = "trino/trino-master";

    // Keytab paths in KDC container
    protected static final String KDC_HDFS_KEYTAB_PATH = "/keytabs/hdfs.keytab";
    protected static final String KDC_HIVE_KEYTAB_PATH = "/keytabs/hive.keytab";
    protected static final String KDC_HTTP_KEYTAB_PATH = "/keytabs/HTTP.keytab";
    protected static final String KDC_MAPRED_KEYTAB_PATH = "/keytabs/mapred.keytab";
    protected static final String KDC_YARN_KEYTAB_PATH = "/keytabs/yarn.keytab";
    protected static final String KDC_TRINO_KEYTAB_PATH = "/keytabs/trino.keytab";

    // Paths where keytabs are mounted in Hadoop container
    protected static final String HADOOP_KEYTAB_DIR = "/etc/security/keytabs";
    protected static final String HADOOP_HDFS_KEYTAB = HADOOP_KEYTAB_DIR + "/hdfs.keytab";

    // Paths in Trino container
    protected static final String TRINO_KEYTAB = "/etc/trino/trino.keytab";
    protected static final String TRINO_CREDENTIAL_CACHE = "/etc/trino/trino-krbcc";

    protected Network network;
    protected KerberosContainer kdc;
    protected HadoopContainer hadoop;
    protected TrinoContainer trino;
    protected Path tempDir;

    /**
     * Represents a Kerberos principal to be created in the KDC.
     */
    public record PrincipalSpec(String principal, String kdcKeytabPath, boolean otherRealm)
    {
        public PrincipalSpec(String principal, String kdcKeytabPath)
        {
            this(principal, kdcKeytabPath, false);
        }
    }

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // 1. Create temp directory for keytabs and config files
        // The Kerberos data must be loaded via bind amounts, because they are accessed when the container is initially starting (the docker copy into happens after startup)
        try {
            tempDir = Files.createTempDirectory("hive-kerberos-env");
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create temp directory", e);
        }

        // 2. Start KDC with principals
        kdc = new KerberosContainer()
                .withNetwork(network)
                .withNetworkAliases(KerberosContainer.HOST_NAME)
                .withPrincipal(HDFS_PRINCIPAL, KDC_HDFS_KEYTAB_PATH)
                .withPrincipal(HIVE_PRINCIPAL, KDC_HIVE_KEYTAB_PATH)
                .withPrincipal(HTTP_PRINCIPAL, KDC_HTTP_KEYTAB_PATH)
                .withPrincipal(MAPRED_PRINCIPAL, KDC_MAPRED_KEYTAB_PATH)
                .withPrincipal(YARN_PRINCIPAL, KDC_YARN_KEYTAB_PATH)
                .withPrincipal(TRINO_PRINCIPAL, KDC_TRINO_KEYTAB_PATH);

        // Add any additional principals from subclasses
        for (PrincipalSpec spec : getAdditionalPrincipals()) {
            if (spec.otherRealm()) {
                kdc.withOtherRealmPrincipal(spec.principal(), spec.kdcKeytabPath());
            }
            else {
                kdc.withPrincipal(spec.principal(), spec.kdcKeytabPath());
            }
        }

        kdc.start();

        // 3. Write keytabs and config files to temp directory (for bind mounting)
        writeKerberosFiles();

        // 4. Create and start any additional containers (e.g., KMS)
        createAdditionalContainers();

        // 5. Start Hadoop container with bind-mounted Kerberos files
        //    The init script runs as part of the entrypoint, before services start
        hadoop = createKerberosHadoopContainer();
        // Enable log following to see init script output
        hadoop.withLogConsumer(outputFrame -> hadoopLog.info(outputFrame.getUtf8String().stripTrailing()));
        hadoop.start();
        waitForHiveMetastoreReady();

        // 6. Execute any post-Hadoop-start operations (e.g., encryption zone setup)
        afterHadoopStart();

        // 7. Start Trino with Kerberos Hive connector
        trino = createTrinoContainer();
        TrinoProductTestContainer.startAndWait(trino);
    }

    private void waitForHiveMetastoreReady()
    {
        long deadlineNanos = System.nanoTime() + HIVE_METASTORE_STARTUP_TIMEOUT.toNanos();
        int consecutiveSuccesses = 0;

        while (System.nanoTime() < deadlineNanos) {
            if (isHiveMetastoreRunningAndReachable()) {
                consecutiveSuccesses++;
                if (consecutiveSuccesses >= HIVE_METASTORE_STABLE_SUCCESS_POLL_COUNT) {
                    return;
                }
            }
            else {
                consecutiveSuccesses = 0;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(HIVE_METASTORE_STARTUP_POLL_INTERVAL.toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Hive metastore readiness", e);
            }
        }

        throw new RuntimeException("Hive metastore did not become stable within " + HIVE_METASTORE_STARTUP_TIMEOUT);
    }

    private boolean isHiveMetastoreRunningAndReachable()
    {
        try {
            Container.ExecResult status = hadoop.execInContainer(
                    "bash",
                    "-lc",
                    "set -euo pipefail; supervisorctl status hive-metastore | grep -q RUNNING; timeout 5 bash -lc 'echo > /dev/tcp/localhost/9083'");
            return status.getExitCode() == 0;
        }
        catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while probing Hive metastore readiness", e);
            }
            return false;
        }
    }

    /**
     * Extension point: Returns additional Kerberos principals to create in the KDC.
     * Override in subclasses to add environment-specific principals.
     *
     * @return list of additional principal specifications (empty by default)
     */
    protected List<PrincipalSpec> getAdditionalPrincipals()
    {
        return List.of();
    }

    /**
     * Extension point: Returns additional properties to add to core-site.xml.
     * These properties are merged with the base Kerberos properties.
     *
     * @return map of additional core-site.xml properties (empty by default)
     */
    protected Map<String, String> getCoreSiteProperties()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns additional properties to add to hdfs-site.xml.
     * These properties are merged with the base Kerberos properties.
     *
     * @return map of additional hdfs-site.xml properties (empty by default)
     */
    protected Map<String, String> getHdfsSiteProperties()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns additional properties to add to the HDFS client configuration.
     * These properties are merged with the base Kerberos HDFS client properties.
     *
     * @return map of additional HDFS client properties (empty by default)
     */
    protected Map<String, String> getHdfsClientSiteProperties()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns additional properties to add to the Trino Hive catalog.
     * These properties are merged with the base Kerberos catalog properties.
     *
     * @return map of additional catalog properties (empty by default)
     */
    protected Map<String, String> getAdditionalCatalogProperties()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns additional catalogs to add to the Trino container.
     * Each entry is a catalog name mapped to its properties.
     *
     * @return map of additional catalogs (empty by default)
     */
    protected Map<String, Map<String, String>> getAdditionalCatalogs()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns additional properties to add to hive-site.xml.
     * These properties are merged with the base Kerberos properties.
     * <p>
     * Useful for adding SQL-standard security settings like {@code hive.users.in.admin.role}.
     *
     * @return map of additional hive-site.xml properties (empty by default)
     */
    protected Map<String, String> getHiveSiteProperties()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns properties for Hive Metastore Kerberos authentication.
     * Override in subclasses to use credential cache instead of keytab.
     *
     * @return map of metastore authentication properties
     */
    protected Map<String, String> getMetastoreAuthenticationProperties()
    {
        return Map.of("hive.metastore.client.keytab", TRINO_KEYTAB);
    }

    /**
     * Extension point: Returns properties for HDFS Kerberos authentication.
     * Override in subclasses to use credential cache instead of keytab.
     *
     * @return map of HDFS authentication properties
     */
    protected Map<String, String> getHdfsAuthenticationProperties()
    {
        return Map.of("hive.hdfs.trino.keytab", TRINO_KEYTAB);
    }

    protected String getMetastoreClientPrincipal()
    {
        return TRINO_PRINCIPAL + "@" + kdc.getRealm();
    }

    protected String getHdfsClientPrincipal()
    {
        return TRINO_PRINCIPAL + "@" + kdc.getRealm();
    }

    protected Map<String, String> getAdditionalTrinoKeytabs()
    {
        return Map.of();
    }

    /**
     * Extension point: Returns whether HDFS impersonation is enabled.
     * Override in subclasses that require impersonation.
     *
     * @return true if impersonation should be enabled, false otherwise
     */
    protected boolean isHdfsImpersonationEnabled()
    {
        return false;
    }

    /**
     * Extension point: Create and start additional containers before Hadoop starts.
     * Override in subclasses to add environment-specific containers (e.g., KMS).
     */
    protected void createAdditionalContainers()
    {
        // Default: no additional containers
    }

    /**
     * Extension point: Execute operations after Hadoop has started.
     * Override in subclasses for post-startup configuration (e.g., encryption zone setup).
     */
    protected void afterHadoopStart()
    {
        // Default: no post-start operations
    }

    /**
     * Extension point: Write additional keytab files to the temp directory.
     * Override in subclasses to write environment-specific keytabs.
     *
     * @param keytabDir the directory where keytabs should be written
     */
    protected void writeAdditionalKeytabs(Path keytabDir)
            throws IOException
    {
        // Default: no additional keytabs
    }

    /**
     * Extension point: Create credential cache files from keytabs.
     * Override in subclasses that use credential caching instead of keytabs.
     * <p>
     * Credential caches are pre-obtained Kerberos tickets that can be used
     * instead of keytabs for authentication.
     *
     * @param keytabDir the directory where credential caches should be written
     */
    protected void writeCredentialCaches(Path keytabDir)
            throws IOException
    {
        // Default: no credential caches needed
    }

    /**
     * Writes Kerberos configuration files and keytabs to the temp directory.
     * These files are then bind-mounted into containers.
     */
    private void writeKerberosFiles()
    {
        try {
            // Create keytab subdirectory
            Path keytabDir = tempDir.resolve("keytabs");
            Files.createDirectories(keytabDir);

            // Write krb5.conf
            Files.writeString(tempDir.resolve("krb5.conf"), kdc.getKrb5Conf());

            // Write keytabs and make them readable by container services
            // When bind-mounted, files keep host permissions, so we make them world-readable
            writeKeytab(keytabDir, "hdfs.keytab", KDC_HDFS_KEYTAB_PATH);
            writeKeytab(keytabDir, "hive.keytab", KDC_HIVE_KEYTAB_PATH);
            writeKeytab(keytabDir, "HTTP.keytab", KDC_HTTP_KEYTAB_PATH);
            writeKeytab(keytabDir, "mapred.keytab", KDC_MAPRED_KEYTAB_PATH);
            writeKeytab(keytabDir, "yarn.keytab", KDC_YARN_KEYTAB_PATH);
            writeKeytab(keytabDir, "trino.keytab", KDC_TRINO_KEYTAB_PATH);

            // Allow subclasses to write additional keytabs
            writeAdditionalKeytabs(keytabDir);

            // Allow subclasses to create credential caches
            writeCredentialCaches(keytabDir);

            // Write init script
            Path initDir = tempDir.resolve("hadoop-init.d");
            Files.createDirectories(initDir);
            Path initScript = initDir.resolve("00-kerberos-init.sh");
            Files.writeString(initScript, generateKerberosInitScript());
            // Make executable
            initScript.toFile().setExecutable(true, false);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write Kerberos files", e);
        }
    }

    /**
     * Writes a keytab file from the KDC to the local filesystem.
     */
    protected void writeKeytab(Path keytabDir, String fileName, String kdcPath)
            throws IOException
    {
        Path keytab = keytabDir.resolve(fileName);
        byte[] keytabBytes = kdc.getKeytab(kdcPath);
        log.debug("%s size from KDC: %d bytes", fileName, keytabBytes.length);
        Files.write(keytab, keytabBytes);
        keytab.toFile().setReadable(true, false);
    }

    private HadoopContainer createKerberosHadoopContainer()
    {
        HadoopContainer container = createKerberosBaseHadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);

        // Bind mount Kerberos files so they're available when entrypoint runs
        container.withFileSystemBind(
                tempDir.resolve("krb5.conf").toString(),
                "/etc/krb5.conf",
                BindMode.READ_ONLY);

        container.withFileSystemBind(
                tempDir.resolve("keytabs").toString(),
                HADOOP_KEYTAB_DIR,
                BindMode.READ_ONLY);

        // Bind mount init script - runs before supervisord starts services
        container.withFileSystemBind(
                tempDir.resolve("hadoop-init.d/00-kerberos-init.sh").toString(),
                "/etc/hadoop-init.d/00-kerberos-init.sh",
                BindMode.READ_ONLY);

        return container;
    }

    /**
     * Extension point for selecting the Hadoop image used by Kerberos environments.
     * Subclasses can override this to select a different pre-baked image variant.
     */
    protected HadoopContainer createKerberosBaseHadoopContainer()
    {
        return HadoopContainer.kerberized();
    }

    /**
     * Generates the Kerberos initialization script that runs before supervisord.
     * <p>
     * The image owns the standard Kerberos configuration. This script only applies
     * properties supplied by specialized product-test environments.
     */
    private String generateKerberosInitScript()
    {
        Map<String, String> hdfsSiteProperties = new LinkedHashMap<>();
        hdfsSiteProperties.put("dfs.client.use.datanode.hostname", "true");
        hdfsSiteProperties.put("dfs.datanode.use.datanode.hostname", "true");
        hdfsSiteProperties.put("dfs.datanode.hostname", HadoopContainer.HOST_NAME);
        hdfsSiteProperties.put("dfs.client.socket-timeout", "180000");
        hdfsSiteProperties.put("dfs.datanode.socket.write.timeout", "600000");
        hdfsSiteProperties.put("dfs.replication", "1");
        hdfsSiteProperties.put("dfs.client.read.shortcircuit", "false");
        hdfsSiteProperties.put("dfs.data.transfer.protection", "authentication");
        hdfsSiteProperties.putAll(getHdfsSiteProperties());

        return """
               #!/bin/bash
               set -euo pipefail

               %1$s
               %2$s
               %3$s
               """.formatted(
                generateSiteXmlUpdate("/opt/hadoop/etc/hadoop/core-site.xml", getCoreSiteProperties()),
                generateSiteXmlUpdate("/opt/hadoop/etc/hadoop/hdfs-site.xml", hdfsSiteProperties),
                generateSiteXmlUpdate("/opt/hive/conf/hive-site.xml", getHiveSiteProperties()));
    }

    private static String generateSiteXmlUpdate(String path, Map<String, String> properties)
    {
        if (properties.isEmpty()) {
            return "";
        }

        StringBuilder xml = new StringBuilder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!xml.isEmpty()) {
                xml.append("\\\n");
            }
            xml.append("<property><name>").append(entry.getKey())
                    .append("</name><value>").append(entry.getValue()).append("</value></property>");
        }

        return """
               sed -i '/<\\/configuration>/i \\
               %s' %s
               """.formatted(xml, path);
    }

    /**
     * Generates HDFS client configuration XML with Kerberos-specific settings.
     * <p>
     * Extends the base HDFS client config with SASL data transfer protection,
     * which is required when connecting to a Kerberos-enabled HDFS cluster.
     */
    private String getKerberosHdfsClientSiteXml()
    {
        StringBuilder properties = new StringBuilder();

        // Base HDFS client properties
        properties.append(
                """
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

                <!-- SASL data transfer protection for Kerberos authentication -->
                <property>
                    <name>dfs.data.transfer.protection</name>
                    <value>authentication</value>
                </property>
                """.formatted(HadoopContainer.HOST_NAME, HadoopContainer.HDFS_NAMENODE_PORT));

        // Add any additional properties from subclasses
        for (Map.Entry<String, String> entry : getHdfsClientSiteProperties().entrySet()) {
            properties.append(
                    """
                    <property>
                        <name>%s</name>
                        <value>%s</value>
                    </property>
                    """.formatted(entry.getKey(), entry.getValue()));
        }

        return """
               <?xml version="1.0" encoding="UTF-8"?>
               <configuration>
               %s</configuration>
               """.formatted(properties);
    }

    private TrinoContainer createTrinoContainer()
    {
        String realm = kdc.getRealm();

        // Build catalog properties
        HiveCatalogPropertiesBuilder catalogProperties = HiveCatalogPropertiesBuilder.hiveCatalog("thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT)
                .withHadoopFileSystem()
                .withPartitionProcedures()
                .withCommonProperties()
                // Metastore Kerberos authentication
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.metastore.service.principal", HIVE_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.principal", getMetastoreClientPrincipal())
                .putAll(getMetastoreAuthenticationProperties())
                // HDFS Kerberos authentication
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", String.valueOf(isHdfsImpersonationEnabled()))
                .put("hive.hdfs.trino.principal", getHdfsClientPrincipal())
                .putAll(getHdfsAuthenticationProperties());

        // Add any additional catalog properties from subclasses
        catalogProperties.putAll(getAdditionalCatalogProperties());

        // Use Kerberos-specific HDFS client config with SASL data transfer protection
        TrinoProductTestContainer.Builder containerBuilder = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withNetworkAlias("trino-master")
                .withHdfsConfiguration(getKerberosHdfsClientSiteXml())
                .withCatalog("hive", catalogProperties.build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"));

        // Add any additional catalogs from subclasses
        for (Map.Entry<String, Map<String, String>> entry : getAdditionalCatalogs().entrySet()) {
            containerBuilder.withCatalog(entry.getKey(), entry.getValue());
        }

        TrinoContainer container = containerBuilder.build();

        // Bind mount Kerberos files for Trino
        // IMPORTANT: We must use bind mounts (withFileSystemBind) here, NOT withCopyToContainer.
        // withCopyToContainer uses "docker cp" which executes AFTER the container starts,
        // but krb5.conf and keytabs must be available when Trino initializes (before startup).
        container.withFileSystemBind(
                tempDir.resolve("krb5.conf").toString(),
                "/etc/krb5.conf",
                BindMode.READ_ONLY);

        container.withFileSystemBind(
                tempDir.resolve("keytabs/trino.keytab").toString(),
                TRINO_KEYTAB,
                BindMode.READ_ONLY);

        for (Map.Entry<String, String> entry : getAdditionalTrinoKeytabs().entrySet()) {
            container.withFileSystemBind(
                    tempDir.resolve("keytabs").resolve(entry.getKey()).toString(),
                    entry.getValue(),
                    BindMode.READ_ONLY);
        }

        // Conditionally bind mount credential cache if it exists
        Path credentialCache = tempDir.resolve("keytabs/trino-krbcc");
        if (Files.exists(credentialCache)) {
            container.withFileSystemBind(
                    credentialCache.toString(),
                    TRINO_CREDENTIAL_CACHE,
                    BindMode.READ_ONLY);
        }

        return container;
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
        closeAdditionalContainers();

        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (kdc != null) {
            kdc.close();
            kdc = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
        if (tempDir != null) {
            try {
                // Recursively delete temp directory
                try (Stream<Path> stream = Files.walk(tempDir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.delete(path);
                                }
                                catch (IOException e) {
                                    // Ignore cleanup failures
                                }
                            });
                }
            }
            catch (IOException e) {
                // Ignore cleanup failures
            }
            tempDir = null;
        }
    }

    /**
     * Extension point: Close any additional containers created by subclasses.
     * Called before closing the standard containers (trino, hadoop, kdc).
     */
    protected void closeAdditionalContainers()
    {
        // Default: no additional containers to close
    }

    /**
     * Returns the Kerberos realm used by this environment.
     */
    public String getKerberosRealm()
    {
        return kdc.getRealm();
    }

    /**
     * Returns the KDC container for advanced configuration.
     */
    public KerberosContainer getKdc()
    {
        return kdc;
    }

    /**
     * Returns the Hadoop container for advanced configuration.
     */
    public HadoopContainer getHadoop()
    {
        return hadoop;
    }
}
