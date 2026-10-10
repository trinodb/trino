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
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Mixed Hive environment with one Kerberos-secured and one standard Hadoop cluster.
 * <p>
 * This environment provides two Hadoop clusters with different security configurations:
 * <ul>
 *   <li>hive1 catalog - Kerberos-secured Hadoop cluster</li>
 *   <li>hive2 catalog - Standard (no auth) Hadoop cluster</li>
 * </ul>
 * <p>
 * This is useful for testing Trino's ability to handle multiple Hive catalogs
 * with different authentication mechanisms simultaneously.
 * <p>
 * <b>Architecture:</b>
 * <pre>
 * ┌────────────────────────────────────────────────────────────────────────────┐
 * │                           Docker Network                                    │
 * │                                                                             │
 * │  ┌─────────────────┐                                                        │
 * │  │ KDC Container   │  Kerberos KDC (only for cluster #1)                   │
 * │  │ (kdc)           │                                                        │
 * │  │   :88           │                                                        │
 * │  └────────┬────────┘                                                        │
 * │           │                                                                 │
 * │           │ (Kerberos)                                                      │
 * │           ▼                                                                 │
 * │  ┌─────────────────────┐    ┌──────────────────────┐                       │
 * │  │ Hadoop Container #1 │    │ Hadoop Container #2  │                       │
 * │  │ (hadoop-master)     │    │ (hadoop-master-2)    │                       │
 * │  │ KERBERIZED          │    │ STANDARD (no auth)   │                       │
 * │  │   HMS: :9083        │    │   HMS: :9083         │                       │
 * │  │   HDFS: :9000       │    │   HDFS: :9000        │                       │
 * │  └─────────────────────┘    └──────────────────────┘                       │
 * │             ▲                          ▲                                    │
 * │             │                          │                                    │
 * │  ┌──────────┴──────────────────────────┴───────────┐                       │
 * │  │                Trino Container                   │                       │
 * │  │  hive1 catalog → hadoop-master (Kerberos auth)  │                       │
 * │  │  hive2 catalog → hadoop-master-2 (no auth)      │                       │
 * │  └─────────────────────────────────────────────────┘                       │
 * └────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * <b>Principals created (Kerberos cluster only):</b>
 * <ul>
 *   <li>hdfs/hadoop-master@REALM - for Hadoop #1 HDFS services</li>
 *   <li>hive/hadoop-master@REALM - for Hadoop #1 Hive Metastore</li>
 *   <li>HTTP/hadoop-master@REALM - for Hadoop #1 WebHDFS/HTTP SPNEGO</li>
 *   <li>trino/trino-master@REALM - for Trino service</li>
 * </ul>
 */
public class TwoMixedHivesEnvironment
        extends ProductTestEnvironment
{
    private static final Logger log = Logger.get(TwoMixedHivesEnvironment.class);
    private static final Logger hadoop1Log = Logger.get("Hadoop1-Kerberos");
    private static final Logger hadoop2Log = Logger.get("Hadoop2-Standard");

    // Hostnames for the two Hadoop clusters
    private static final String HADOOP1_HOST = HadoopContainer.HOST_NAME;  // "hadoop-master" (kerberized)
    private static final String HADOOP2_HOST = "hadoop-master-2";          // (standard)

    // Principal names for Kerberos cluster (without realm)
    private static final String HDFS_PRINCIPAL = "hdfs/" + HADOOP1_HOST;
    private static final String HIVE_PRINCIPAL = "hive/" + HADOOP1_HOST;
    private static final String HTTP_PRINCIPAL = "HTTP/" + HADOOP1_HOST;
    private static final String MAPRED_PRINCIPAL = "mapred/" + HADOOP1_HOST;
    private static final String YARN_PRINCIPAL = "yarn/" + HADOOP1_HOST;
    private static final String TRINO_PRINCIPAL = "trino/trino-master";

    // Keytab paths in KDC container
    private static final String KDC_HDFS_KEYTAB_PATH = "/keytabs/hdfs.keytab";
    private static final String KDC_HIVE_KEYTAB_PATH = "/keytabs/hive.keytab";
    private static final String KDC_HTTP_KEYTAB_PATH = "/keytabs/HTTP.keytab";
    private static final String KDC_MAPRED_KEYTAB_PATH = "/keytabs/mapred.keytab";
    private static final String KDC_YARN_KEYTAB_PATH = "/keytabs/yarn.keytab";
    private static final String KDC_TRINO_KEYTAB_PATH = "/keytabs/trino.keytab";

    // Paths where keytabs are mounted in Hadoop container
    private static final String HADOOP_KEYTAB_DIR = "/etc/security/keytabs";

    // Paths in Trino container
    private static final String TRINO_KEYTAB = "/etc/trino/trino.keytab";

    private Network network;
    private KerberosContainer kdc;
    private HadoopContainer hadoop1;  // Kerberized
    private HadoopContainer hadoop2;  // Standard
    private TrinoContainer trino;
    private Path tempDir;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Create temp directory for keytabs and config files
        try {
            tempDir = Files.createTempDirectory("two-mixed-hives-env");
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create temp directory", e);
        }

        // 1. Start KDC with principals for the Kerberos cluster
        kdc = new KerberosContainer()
                .withNetwork(network)
                .withNetworkAliases(KerberosContainer.HOST_NAME)
                .withPrincipal(HDFS_PRINCIPAL, KDC_HDFS_KEYTAB_PATH)
                .withPrincipal(HIVE_PRINCIPAL, KDC_HIVE_KEYTAB_PATH)
                .withPrincipal(HTTP_PRINCIPAL, KDC_HTTP_KEYTAB_PATH)
                .withPrincipal(MAPRED_PRINCIPAL, KDC_MAPRED_KEYTAB_PATH)
                .withPrincipal(YARN_PRINCIPAL, KDC_YARN_KEYTAB_PATH)
                .withPrincipal(TRINO_PRINCIPAL, KDC_TRINO_KEYTAB_PATH);
        kdc.start();

        // 2. Write keytabs and config files
        writeKerberosFiles();

        // 3. Start Hadoop cluster 1 (Kerberized)
        hadoop1 = createKerberosHadoopContainer();
        hadoop1.withLogConsumer(outputFrame -> hadoop1Log.info(outputFrame.getUtf8String().stripTrailing()));
        hadoop1.start();

        // 4. Start Hadoop cluster 2 (Standard - no Kerberos)
        hadoop2 = HadoopContainer.withHostName(HADOOP2_HOST)
                .withNetwork(network)
                .withNetworkAliases(HADOOP2_HOST);
        hadoop2.withLogConsumer(outputFrame -> hadoop2Log.info(outputFrame.getUtf8String().stripTrailing()));
        hadoop2.start();

        // 5. Start Trino with mixed auth Hive catalogs
        trino = createTrinoContainer();
        TrinoProductTestContainer.startAndWait(trino);
    }

    private void writeKerberosFiles()
    {
        try {
            // Write krb5.conf
            Files.writeString(tempDir.resolve("krb5.conf"), kdc.getKrb5Conf());

            // Create keytab directory
            Path keytabDir = tempDir.resolve("keytabs");
            Path trinoKeytabDir = tempDir.resolve("trino-keytabs");
            Files.createDirectories(keytabDir);
            Files.createDirectories(trinoKeytabDir);

            // Write Hadoop cluster 1 keytabs
            writeKeytab(keytabDir, "hdfs.keytab", KDC_HDFS_KEYTAB_PATH);
            writeKeytab(keytabDir, "hive.keytab", KDC_HIVE_KEYTAB_PATH);
            writeKeytab(keytabDir, "HTTP.keytab", KDC_HTTP_KEYTAB_PATH);
            writeKeytab(keytabDir, "mapred.keytab", KDC_MAPRED_KEYTAB_PATH);
            writeKeytab(keytabDir, "yarn.keytab", KDC_YARN_KEYTAB_PATH);

            // Write Trino keytab
            writeKeytab(trinoKeytabDir, "trino.keytab", KDC_TRINO_KEYTAB_PATH);

            // Write init script for kerberized cluster
            Path initDir = tempDir.resolve("hadoop-init.d");
            Files.createDirectories(initDir);

            Path initScript = initDir.resolve("00-kerberos-init.sh");
            Files.writeString(initScript, generateKerberosInitScript());
            initScript.toFile().setExecutable(true, false);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write Kerberos files", e);
        }
    }

    private void writeKeytab(Path keytabDir, String fileName, String kdcPath)
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
        HadoopContainer container = HadoopContainer.kerberized()
                .withNetwork(network)
                .withNetworkAliases(HADOOP1_HOST);

        // Bind mount Kerberos files
        container.withFileSystemBind(
                tempDir.resolve("krb5.conf").toString(),
                "/etc/krb5.conf",
                BindMode.READ_ONLY);

        container.withFileSystemBind(
                tempDir.resolve("keytabs").toString(),
                HADOOP_KEYTAB_DIR,
                BindMode.READ_ONLY);

        // Bind mount init script
        container.withFileSystemBind(
                tempDir.resolve("hadoop-init.d/00-kerberos-init.sh").toString(),
                "/etc/hadoop-init.d/00-kerberos-init.sh",
                BindMode.READ_ONLY);

        return container;
    }

    private static String generateKerberosInitScript()
    {
        return """
               #!/bin/bash
               set -euo pipefail

               sed -i '/<\\/configuration>/i \\
               <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>\\
               <property><name>dfs.datanode.use.datanode.hostname</name><value>true</value></property>\\
               <property><name>dfs.datanode.hostname</name><value>%s</value></property>\\
               <property><name>dfs.client.socket-timeout</name><value>180000</value></property>\\
               <property><name>dfs.datanode.socket.write.timeout</name><value>600000</value></property>\\
               <property><name>dfs.replication</name><value>1</value></property>\\
               <property><name>dfs.client.read.shortcircuit</name><value>false</value></property>\\
               <property><name>dfs.data.transfer.protection</name><value>authentication</value></property>' \\
                   /opt/hadoop/etc/hadoop/hdfs-site.xml
               """.formatted(HADOOP1_HOST);
    }

    private String getKerberosHdfsClientSiteXml()
    {
        return """
               <?xml version="1.0" encoding="UTF-8"?>
               <configuration>
                   <property>
                       <name>fs.defaultFS</name>
                       <value>hdfs://%1$s:%2$d</value>
                   </property>
                   <property>
                       <name>dfs.client.use.datanode.hostname</name>
                       <value>true</value>
                   </property>
                   <property>
                       <name>dfs.datanode.use.datanode.hostname</name>
                       <value>true</value>
                   </property>
                   <property>
                       <name>dfs.client.socket-timeout</name>
                       <value>180000</value>
                   </property>
                   <property>
                       <name>dfs.datanode.socket.write.timeout</name>
                       <value>600000</value>
                   </property>
                   <property>
                       <name>dfs.replication</name>
                       <value>1</value>
                   </property>
                   <property>
                       <name>dfs.client.read.shortcircuit</name>
                       <value>false</value>
                   </property>
                   <property>
                       <name>dfs.data.transfer.protection</name>
                       <value>authentication</value>
                   </property>
               </configuration>
               """.formatted(HADOOP1_HOST, HadoopContainer.HDFS_NAMENODE_PORT);
    }

    private TrinoContainer createTrinoContainer()
    {
        String realm = kdc.getRealm();

        // Catalog properties for hive1 (Kerberos)
        Map<String, String> hive1CatalogProperties = HiveCatalogPropertiesBuilder.hiveCatalog("thrift://" + HADOOP1_HOST + ":" + HadoopContainer.HIVE_METASTORE_PORT)
                .put("fs.hadoop.enabled", "true")
                .put("hive.config.resources", "/etc/trino/hdfs-site-1.xml")
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.metastore.service.principal", HIVE_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.principal", TRINO_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.keytab", TRINO_KEYTAB)
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "false")
                .put("hive.hdfs.trino.principal", TRINO_PRINCIPAL + "@" + realm)
                .put("hive.hdfs.trino.keytab", TRINO_KEYTAB)
                .build();

        // Catalog properties for hive2 (Standard - no Kerberos)
        Map<String, String> hive2CatalogProperties = HiveCatalogPropertiesBuilder.hiveCatalog("thrift://" + HADOOP2_HOST + ":" + HadoopContainer.HIVE_METASTORE_PORT)
                .put("fs.hadoop.enabled", "true")
                .put("hive.config.resources", "/etc/trino/hdfs-site-2.xml")
                .build();

        TrinoContainer container = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withNetworkAlias("trino-master")
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("hive1", hive1CatalogProperties)
                .withCatalog("hive2", hive2CatalogProperties)
                .build();

        // Copy HDFS configurations for both clusters
        container.withCopyToContainer(
                Transferable.of(getKerberosHdfsClientSiteXml()),
                "/etc/trino/hdfs-site-1.xml");
        container.withCopyToContainer(
                Transferable.of(hadoop2.getHdfsClientSiteXml()),
                "/etc/trino/hdfs-site-2.xml");

        // Bind mount Kerberos files for Trino
        container.withFileSystemBind(
                tempDir.resolve("krb5.conf").toString(),
                "/etc/krb5.conf",
                BindMode.READ_ONLY);

        container.withFileSystemBind(
                tempDir.resolve("trino-keytabs/trino.keytab").toString(),
                TRINO_KEYTAB,
                BindMode.READ_ONLY);

        return container;
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
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
        if (hadoop2 != null) {
            hadoop2.close();
            hadoop2 = null;
        }
        if (hadoop1 != null) {
            hadoop1.close();
            hadoop1 = null;
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
     * Returns the KDC container.
     */
    public KerberosContainer getKdc()
    {
        return kdc;
    }

    /**
     * Returns the first Hadoop container (Kerberized).
     */
    public HadoopContainer getKerberizedHadoop()
    {
        return hadoop1;
    }

    /**
     * Returns the second Hadoop container (Standard - no auth).
     */
    public HadoopContainer getStandardHadoop()
    {
        return hadoop2;
    }
}
