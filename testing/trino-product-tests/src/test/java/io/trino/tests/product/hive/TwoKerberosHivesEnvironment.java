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
 * Dual Kerberos-enabled Hive environment.
 * <p>
 * This environment provides two separate Kerberos-secured Hadoop clusters
 * accessible via two different Trino catalogs (hive1 and hive2).
 * <p>
 * <b>Architecture:</b>
 * <pre>
 * ┌────────────────────────────────────────────────────────────────────────────┐
 * │                           Docker Network                                    │
 * │                                                                             │
 * │  ┌─────────────────┐                                                        │
 * │  │ KDC Container   │  Shared Kerberos KDC for both clusters                │
 * │  │ (kdc)           │                                                        │
 * │  │   :88           │                                                        │
 * │  └────────┬────────┘                                                        │
 * │           │                                                                 │
 * │     ┌─────┴─────┐                                                          │
 * │     │           │                                                          │
 * │  ┌──▼──────────────────┐    ┌──────────────────────┐                       │
 * │  │ Hadoop Container #1 │    │ Hadoop Container #2  │                       │
 * │  │ (hadoop-master)     │    │ (hadoop-master-2)    │                       │
 * │  │   HMS: :9083        │    │   HMS: :9083         │                       │
 * │  │   HDFS: :9000       │    │   HDFS: :9000        │                       │
 * │  └─────────────────────┘    └──────────────────────┘                       │
 * │             ▲                          ▲                                    │
 * │             │                          │                                    │
 * │  ┌──────────┴──────────────────────────┴───────────┐                       │
 * │  │                Trino Container                   │                       │
 * │  │  hive1 catalog → hadoop-master (Kerberos auth)  │                       │
 * │  │  hive2 catalog → hadoop-master-2 (Kerberos auth)│                       │
 * │  └─────────────────────────────────────────────────┘                       │
 * └────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * <b>Principals created:</b>
 * <ul>
 *   <li>hdfs/hadoop-master@REALM - for Hadoop #1 HDFS services</li>
 *   <li>hive/hadoop-master@REALM - for Hadoop #1 Hive Metastore</li>
 *   <li>HTTP/hadoop-master@REALM - for Hadoop #1 WebHDFS/HTTP SPNEGO</li>
 *   <li>mapred/hadoop-master@REALM - for Hadoop #1 MapReduce services</li>
 *   <li>yarn/hadoop-master@REALM - for Hadoop #1 YARN services</li>
 *   <li>hdfs/hadoop-master-2@REALM - for Hadoop #2 HDFS services</li>
 *   <li>hive/hadoop-master-2@REALM - for Hadoop #2 Hive Metastore</li>
 *   <li>HTTP/hadoop-master-2@REALM - for Hadoop #2 WebHDFS/HTTP SPNEGO</li>
 *   <li>mapred/hadoop-master-2@REALM - for Hadoop #2 MapReduce services</li>
 *   <li>yarn/hadoop-master-2@REALM - for Hadoop #2 YARN services</li>
 *   <li>trino/trino-master@REALM - for Trino service</li>
 * </ul>
 */
public class TwoKerberosHivesEnvironment
        extends ProductTestEnvironment
{
    private static final Logger log = Logger.get(TwoKerberosHivesEnvironment.class);
    private static final Logger hadoop1Log = Logger.get("Hadoop1");
    private static final Logger hadoop2Log = Logger.get("Hadoop2");

    // Hostnames for the two Hadoop clusters
    private static final String HADOOP1_HOST = HadoopContainer.HOST_NAME;  // "hadoop-master"
    private static final String HADOOP2_HOST = "hadoop-master-2";

    // Principal names (without realm)
    private static final String HDFS1_PRINCIPAL = "hdfs/" + HADOOP1_HOST;
    private static final String HIVE1_PRINCIPAL = "hive/" + HADOOP1_HOST;
    private static final String HTTP1_PRINCIPAL = "HTTP/" + HADOOP1_HOST;
    private static final String MAPRED1_PRINCIPAL = "mapred/" + HADOOP1_HOST;
    private static final String YARN1_PRINCIPAL = "yarn/" + HADOOP1_HOST;
    private static final String HDFS2_PRINCIPAL = "hdfs/" + HADOOP2_HOST;
    private static final String HIVE2_PRINCIPAL = "hive/" + HADOOP2_HOST;
    private static final String HTTP2_PRINCIPAL = "HTTP/" + HADOOP2_HOST;
    private static final String MAPRED2_PRINCIPAL = "mapred/" + HADOOP2_HOST;
    private static final String YARN2_PRINCIPAL = "yarn/" + HADOOP2_HOST;
    private static final String TRINO_PRINCIPAL = "trino/trino-master";

    // Keytab paths in KDC container
    private static final String KDC_HDFS1_KEYTAB_PATH = "/keytabs/hdfs1.keytab";
    private static final String KDC_HIVE1_KEYTAB_PATH = "/keytabs/hive1.keytab";
    private static final String KDC_HTTP1_KEYTAB_PATH = "/keytabs/HTTP1.keytab";
    private static final String KDC_MAPRED1_KEYTAB_PATH = "/keytabs/mapred1.keytab";
    private static final String KDC_YARN1_KEYTAB_PATH = "/keytabs/yarn1.keytab";
    private static final String KDC_HDFS2_KEYTAB_PATH = "/keytabs/hdfs2.keytab";
    private static final String KDC_HIVE2_KEYTAB_PATH = "/keytabs/hive2.keytab";
    private static final String KDC_HTTP2_KEYTAB_PATH = "/keytabs/HTTP2.keytab";
    private static final String KDC_MAPRED2_KEYTAB_PATH = "/keytabs/mapred2.keytab";
    private static final String KDC_YARN2_KEYTAB_PATH = "/keytabs/yarn2.keytab";
    private static final String KDC_TRINO_KEYTAB_PATH = "/keytabs/trino.keytab";

    // Path where keytabs are mounted in Hadoop containers
    private static final String HADOOP_KEYTAB_DIR = "/etc/security/keytabs";

    // Paths in Trino container
    private static final String TRINO_KEYTAB = "/etc/trino/trino.keytab";

    private Network network;
    private KerberosContainer kdc;
    private HadoopContainer hadoop1;
    private HadoopContainer hadoop2;
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
            tempDir = Files.createTempDirectory("two-kerberos-hives-env");
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create temp directory", e);
        }

        // 1. Start KDC with principals for both Hadoop clusters
        kdc = new KerberosContainer()
                .withNetwork(network)
                .withNetworkAliases(KerberosContainer.HOST_NAME)
                // Hadoop cluster 1 principals
                .withPrincipal(HDFS1_PRINCIPAL, KDC_HDFS1_KEYTAB_PATH)
                .withPrincipal(HIVE1_PRINCIPAL, KDC_HIVE1_KEYTAB_PATH)
                .withPrincipal(HTTP1_PRINCIPAL, KDC_HTTP1_KEYTAB_PATH)
                .withPrincipal(MAPRED1_PRINCIPAL, KDC_MAPRED1_KEYTAB_PATH)
                .withPrincipal(YARN1_PRINCIPAL, KDC_YARN1_KEYTAB_PATH)
                // Hadoop cluster 2 principals
                .withPrincipal(HDFS2_PRINCIPAL, KDC_HDFS2_KEYTAB_PATH)
                .withPrincipal(HIVE2_PRINCIPAL, KDC_HIVE2_KEYTAB_PATH)
                .withPrincipal(HTTP2_PRINCIPAL, KDC_HTTP2_KEYTAB_PATH)
                .withPrincipal(MAPRED2_PRINCIPAL, KDC_MAPRED2_KEYTAB_PATH)
                .withPrincipal(YARN2_PRINCIPAL, KDC_YARN2_KEYTAB_PATH)
                // Trino principal
                .withPrincipal(TRINO_PRINCIPAL, KDC_TRINO_KEYTAB_PATH);
        kdc.start();

        // 2. Write keytabs and config files
        writeKerberosFiles();

        // 3. Start Hadoop cluster 1
        hadoop1 = createKerberosHadoopContainer(
                HADOOP1_HOST,
                "keytabs1",
                "hadoop1-init.d");
        hadoop1.withLogConsumer(outputFrame -> hadoop1Log.info(outputFrame.getUtf8String().stripTrailing()));
        hadoop1.start();

        // 4. Start Hadoop cluster 2
        hadoop2 = createKerberosHadoopContainer(
                HADOOP2_HOST,
                "keytabs2",
                "hadoop2-init.d");
        hadoop2.withLogConsumer(outputFrame -> hadoop2Log.info(outputFrame.getUtf8String().stripTrailing()));
        hadoop2.start();

        // 5. Start Trino with two Kerberos Hive catalogs
        trino = createTrinoContainer();
        TrinoProductTestContainer.startAndWait(trino);
    }

    private void writeKerberosFiles()
    {
        try {
            // Write krb5.conf
            Files.writeString(tempDir.resolve("krb5.conf"), kdc.getKrb5Conf());

            // Create keytab directories for both clusters
            Path keytab1Dir = tempDir.resolve("keytabs1");
            Path keytab2Dir = tempDir.resolve("keytabs2");
            Path trinoKeytabDir = tempDir.resolve("trino-keytabs");
            Files.createDirectories(keytab1Dir);
            Files.createDirectories(keytab2Dir);
            Files.createDirectories(trinoKeytabDir);

            // Write cluster 1 keytabs (named as expected by Hadoop)
            writeKeytab(keytab1Dir, "hdfs.keytab", KDC_HDFS1_KEYTAB_PATH);
            writeKeytab(keytab1Dir, "hive.keytab", KDC_HIVE1_KEYTAB_PATH);
            writeKeytab(keytab1Dir, "HTTP.keytab", KDC_HTTP1_KEYTAB_PATH);
            writeKeytab(keytab1Dir, "mapred.keytab", KDC_MAPRED1_KEYTAB_PATH);
            writeKeytab(keytab1Dir, "yarn.keytab", KDC_YARN1_KEYTAB_PATH);

            // Write cluster 2 keytabs (named as expected by Hadoop)
            writeKeytab(keytab2Dir, "hdfs.keytab", KDC_HDFS2_KEYTAB_PATH);
            writeKeytab(keytab2Dir, "hive.keytab", KDC_HIVE2_KEYTAB_PATH);
            writeKeytab(keytab2Dir, "HTTP.keytab", KDC_HTTP2_KEYTAB_PATH);
            writeKeytab(keytab2Dir, "mapred.keytab", KDC_MAPRED2_KEYTAB_PATH);
            writeKeytab(keytab2Dir, "yarn.keytab", KDC_YARN2_KEYTAB_PATH);

            // Write Trino keytab
            writeKeytab(trinoKeytabDir, "trino.keytab", KDC_TRINO_KEYTAB_PATH);

            // Write init scripts for both clusters
            Path init1Dir = tempDir.resolve("hadoop1-init.d");
            Path init2Dir = tempDir.resolve("hadoop2-init.d");
            Files.createDirectories(init1Dir);
            Files.createDirectories(init2Dir);

            Path initScript1 = init1Dir.resolve("00-kerberos-init.sh");
            Files.writeString(initScript1, generateKerberosInitScript(HADOOP1_HOST));
            initScript1.toFile().setExecutable(true, false);

            Path initScript2 = init2Dir.resolve("00-kerberos-init.sh");
            Files.writeString(initScript2, generateKerberosInitScript(HADOOP2_HOST));
            initScript2.toFile().setExecutable(true, false);
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

    private HadoopContainer createKerberosHadoopContainer(String hostName, String keytabDirName, String initDirName)
    {
        HadoopContainer container = HadoopContainer.kerberizedWithHostName(hostName)
                .withNetwork(network)
                .withNetworkAliases(hostName);

        // Bind mount Kerberos files
        container.withFileSystemBind(
                tempDir.resolve("krb5.conf").toString(),
                "/etc/krb5.conf",
                BindMode.READ_ONLY);

        container.withFileSystemBind(
                tempDir.resolve(keytabDirName).toString(),
                HADOOP_KEYTAB_DIR,
                BindMode.READ_ONLY);

        // Bind mount init script
        container.withFileSystemBind(
                tempDir.resolve(initDirName + "/00-kerberos-init.sh").toString(),
                "/etc/hadoop-init.d/00-kerberos-init.sh",
                BindMode.READ_ONLY);

        return container;
    }

    private String generateKerberosInitScript(String hostName)
    {
        return """
               #!/bin/bash
               set -euo pipefail

               sed -i 's/%1$s/%2$s/g' \
                   /opt/hadoop/etc/hadoop/*-site.xml \
                   /opt/hive/conf/*-site.xml \
                   /etc/hadoop-init.d/init-hdfs.sh

               sed -i '/<\\/configuration>/i \\
               <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>\\
               <property><name>dfs.datanode.use.datanode.hostname</name><value>true</value></property>\\
               <property><name>dfs.datanode.hostname</name><value>%2$s</value></property>\\
               <property><name>dfs.client.socket-timeout</name><value>180000</value></property>\\
               <property><name>dfs.datanode.socket.write.timeout</name><value>600000</value></property>\\
               <property><name>dfs.replication</name><value>1</value></property>\\
               <property><name>dfs.client.read.shortcircuit</name><value>false</value></property>\\
               <property><name>dfs.data.transfer.protection</name><value>authentication</value></property>' \\
                   /opt/hadoop/etc/hadoop/hdfs-site.xml
               """.formatted(HadoopContainer.HOST_NAME, hostName);
    }

    private String getKerberosHdfsClientSiteXml(String hostName)
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
               """.formatted(hostName, HadoopContainer.HDFS_NAMENODE_PORT);
    }

    private TrinoContainer createTrinoContainer()
    {
        String realm = kdc.getRealm();

        // Build catalog properties for hive1
        Map<String, String> hive1CatalogProperties = HiveCatalogPropertiesBuilder.hiveCatalog("thrift://" + HADOOP1_HOST + ":" + HadoopContainer.HIVE_METASTORE_PORT)
                .put("fs.hadoop.enabled", "true")
                .put("hive.config.resources", "/etc/trino/hdfs-site-1.xml")
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.metastore.service.principal", HIVE1_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.principal", TRINO_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.keytab", TRINO_KEYTAB)
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "false")
                .put("hive.hdfs.trino.principal", TRINO_PRINCIPAL + "@" + realm)
                .put("hive.hdfs.trino.keytab", TRINO_KEYTAB)
                .build();

        // Build catalog properties for hive2
        Map<String, String> hive2CatalogProperties = HiveCatalogPropertiesBuilder.hiveCatalog("thrift://" + HADOOP2_HOST + ":" + HadoopContainer.HIVE_METASTORE_PORT)
                .put("fs.hadoop.enabled", "true")
                .put("hive.config.resources", "/etc/trino/hdfs-site-2.xml")
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.metastore.service.principal", HIVE2_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.principal", TRINO_PRINCIPAL + "@" + realm)
                .put("hive.metastore.client.keytab", TRINO_KEYTAB)
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "false")
                .put("hive.hdfs.trino.principal", TRINO_PRINCIPAL + "@" + realm)
                .put("hive.hdfs.trino.keytab", TRINO_KEYTAB)
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
                Transferable.of(getKerberosHdfsClientSiteXml(HADOOP1_HOST)),
                "/etc/trino/hdfs-site-1.xml");
        container.withCopyToContainer(
                Transferable.of(getKerberosHdfsClientSiteXml(HADOOP2_HOST)),
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
     * Returns the first Hadoop container.
     */
    public HadoopContainer getHadoop1()
    {
        return hadoop1;
    }

    /**
     * Returns the second Hadoop container.
     */
    public HadoopContainer getHadoop2()
    {
        return hadoop2;
    }
}
