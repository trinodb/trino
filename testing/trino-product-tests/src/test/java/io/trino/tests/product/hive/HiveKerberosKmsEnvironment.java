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
import io.trino.testing.containers.KmsContainer;
import org.testcontainers.containers.BindMode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Kerberos-enabled Hive environment with HDFS encryption via KMS.
 * <p>
 * This environment extends {@link HiveKerberosEnvironment} to add support for
 * HDFS Transparent Data Encryption (TDE) using Hadoop KMS (Key Management Server).
 * <p>
 * <b>Additional Components:</b>
 * <ul>
 *   <li>KMS container - Key Management Server for encryption keys</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                        Docker Network                               │
 * │                                                                     │
 * │  ┌─────────┐  ┌─────────────────────┐  ┌────────────────────────┐  │
 * │  │   KDC   │  │   Hadoop Container  │  │    KMS Container       │  │
 * │  │         │◄─│   (HDFS + Hive)     │──│   (Key Management)     │  │
 * │  │  :88    │  │   :9000, :9083      │  │   :9600                │  │
 * │  └─────────┘  └─────────────────────┘  └────────────────────────┘  │
 * │       ▲              ▲                           ▲                 │
 * │       │              │                           │                 │
 * │  ┌────┴──────────────┴───────────────────────────┴──────────────┐  │
 * │  │                      Trino Container                          │  │
 * │  │  Hive catalog with Kerberos auth + encryption zone support    │  │
 * │  └───────────────────────────────────────────────────────────────┘  │
 * └─────────────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * <b>Additional Principals:</b>
 * <ul>
 *   <li>HTTP/kms@REALM - KMS SPNEGO authentication</li>
 * </ul>
 */
public class HiveKerberosKmsEnvironment
        extends HiveKerberosEnvironment
{
    private static final Logger log = Logger.get(HiveKerberosKmsEnvironment.class);
    private static final Logger kmsLog = Logger.get("KMS");

    // KMS-specific principal
    private static final String HTTP_KMS_PRINCIPAL = "HTTP/kms";
    private static final String KDC_HTTP_KMS_KEYTAB_PATH = "/keytabs/http-kms.keytab";

    // Paths where keytabs are mounted in KMS container
    private static final String KMS_KEYTAB_DIR = "/etc/security/keytabs";
    private static final String KMS_HTTP_KEYTAB = KMS_KEYTAB_DIR + "/http-kms.keytab";

    // Encryption key name
    private static final String ENCRYPTION_KEY_NAME = "test_encryption_key";

    private KmsContainer kms;

    @Override
    protected List<PrincipalSpec> getAdditionalPrincipals()
    {
        return List.of(new PrincipalSpec(HTTP_KMS_PRINCIPAL, KDC_HTTP_KMS_KEYTAB_PATH));
    }

    @Override
    protected Map<String, String> getCoreSiteProperties()
    {
        String kmsUrl = "kms://http@" + KmsContainer.HOST_NAME + ":" + KmsContainer.KMS_PORT + "/kms";
        return Map.of("hadoop.security.key.provider.path", kmsUrl);
    }

    @Override
    protected Map<String, String> getHdfsSiteProperties()
    {
        String kmsUrl = "kms://http@" + KmsContainer.HOST_NAME + ":" + KmsContainer.KMS_PORT + "/kms";
        return Map.of("dfs.encryption.key.provider.uri", kmsUrl);
    }

    @Override
    protected Map<String, String> getHdfsClientSiteProperties()
    {
        String kmsUrl = "kms://http@" + KmsContainer.HOST_NAME + ":" + KmsContainer.KMS_PORT + "/kms";
        return Map.of(
                "hadoop.security.key.provider.path", kmsUrl,
                "dfs.encryption.key.provider.uri", kmsUrl);
    }

    @Override
    protected Map<String, Map<String, String>> getAdditionalCatalogs()
    {
        return Map.of("tpch", Map.of("connector.name", "tpch"));
    }

    @Override
    protected void writeAdditionalKeytabs(Path keytabDir)
            throws IOException
    {
        writeKeytab(keytabDir, "http-kms.keytab", KDC_HTTP_KMS_KEYTAB_PATH);
    }

    @Override
    protected void createAdditionalContainers()
    {
        kms = createKmsContainer();
        kms.withLogConsumer(outputFrame -> kmsLog.info(outputFrame.getUtf8String().stripTrailing()));
        kms.start();
    }

    @Override
    protected void afterHadoopStart()
    {
        setupEncryptionZone();
    }

    @Override
    protected void closeAdditionalContainers()
    {
        if (kms != null) {
            kms.close();
            kms = null;
        }
    }

    private KmsContainer createKmsContainer()
    {
        String realm = kdc.getRealm();

        KmsContainer container = new KmsContainer()
                .withNetwork(network)
                .withNetworkAliases(KmsContainer.HOST_NAME)
                .withKerberosEnabled()
                .withKmsSiteXml(generateKmsSiteXml(realm))
                .withCoreSiteXml(generateKmsCoreSiteXml());

        // Set JAVA_TOOL_OPTIONS for Kerberos
        container.withEnv("JAVA_TOOL_OPTIONS", "-Djava.security.krb5.conf=/etc/krb5.conf");

        // Bind mount Kerberos files
        container.withFileSystemBind(
                tempDir.resolve("krb5.conf").toString(),
                "/etc/krb5.conf",
                BindMode.READ_ONLY);

        container.withFileSystemBind(
                tempDir.resolve("keytabs/http-kms.keytab").toString(),
                KMS_HTTP_KEYTAB,
                BindMode.READ_ONLY);

        return container;
    }

    private String generateKmsSiteXml(String realm)
    {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <configuration>
                    <!-- KMS authentication -->
                    <property>
                        <name>hadoop.kms.authentication.type</name>
                        <value>kerberos</value>
                    </property>
                    <property>
                        <name>hadoop.kms.authentication.kerberos.principal</name>
                        <value>HTTP/kms@%1$s</value>
                    </property>
                    <property>
                        <name>hadoop.kms.authentication.kerberos.keytab</name>
                        <value>%2$s</value>
                    </property>
                    <property>
                        <name>hadoop.kms.authentication.kerberos.name.rules</name>
                        <value>DEFAULT</value>
                    </property>

                    <!-- JavaKeyStoreProvider for key storage -->
                    <property>
                        <name>hadoop.kms.key.provider.uri</name>
                        <value>jceks://file@/var/lib/kms/keys.jceks</value>
                    </property>

                    <!-- ACLs - allow all operations for testing -->
                    <property>
                        <name>hadoop.kms.acl.CREATE</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.DELETE</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.ROLLOVER</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.GET</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.GET_KEYS</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.GET_METADATA</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.SET_KEY_MATERIAL</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.GENERATE_EEK</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.acl.DECRYPT_EEK</name>
                        <value>*</value>
                    </property>
                </configuration>
                """.formatted(realm, KMS_HTTP_KEYTAB);
    }

    private static String generateKmsCoreSiteXml()
    {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <configuration>
                    <property>
                        <name>hadoop.security.authentication</name>
                        <value>kerberos</value>
                    </property>
                    <property>
                        <name>hadoop.security.authorization</name>
                        <value>true</value>
                    </property>

                    <!-- Proxy user settings for hive -->
                    <property>
                        <name>hadoop.kms.proxyuser.hive.hosts</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.proxyuser.hive.groups</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.proxyuser.hive.users</name>
                        <value>*</value>
                    </property>

                    <!-- Proxy user settings for trino -->
                    <property>
                        <name>hadoop.kms.proxyuser.trino.hosts</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.proxyuser.trino.groups</name>
                        <value>*</value>
                    </property>
                    <property>
                        <name>hadoop.kms.proxyuser.trino.users</name>
                        <value>*</value>
                    </property>
                </configuration>
                """;
    }

    private void setupEncryptionZone()
    {
        try {
            // Wait a bit for HDFS to be fully ready
            Thread.sleep(5000);

            // Authenticate as hdfs user
            String realm = kdc.getRealm();
            hadoop.execInContainer("kinit", "-kt",
                    HADOOP_HDFS_KEYTAB,
                    HDFS_PRINCIPAL + "@" + realm);

            // Create encryption key in KMS
            log.info("Creating encryption key: %s", ENCRYPTION_KEY_NAME);
            var createKeyResult = hadoop.execInContainer(
                    "/opt/hadoop/bin/hadoop",
                    "key", "create", ENCRYPTION_KEY_NAME);
            if (createKeyResult.getExitCode() != 0) {
                log.warn("Failed to create encryption key (may already exist): %s", createKeyResult.getStderr());
            }

            // Create directory for encryption zone
            log.info("Creating encryption zone directory");
            var mkdirResult = hadoop.execInContainer(
                    "/opt/hadoop/bin/hdfs",
                    "dfs", "-mkdir", "-p", "/user/hive/encrypted");
            if (mkdirResult.getExitCode() != 0) {
                throw new RuntimeException("Failed to create encryption zone directory: " + mkdirResult.getStderr());
            }

            // Create encryption zone
            log.info("Creating encryption zone");
            var ezResult = hadoop.execInContainer(
                    "/opt/hadoop/bin/hdfs",
                    "crypto", "-createZone", "-keyName", ENCRYPTION_KEY_NAME, "-path", "/user/hive/encrypted");
            if (ezResult.getExitCode() != 0) {
                log.warn("Failed to create encryption zone (may already exist): %s", ezResult.getStderr());
            }

            // Set permissions
            hadoop.execInContainer(
                    "/opt/hadoop/bin/hdfs",
                    "dfs", "-chmod", "-R", "777", "/user/hive/encrypted");

            log.info("Encryption zone setup complete");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while setting up encryption zone", e);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to setup encryption zone", e);
        }
    }

    /**
     * Returns the KMS container.
     */
    public KmsContainer getKms()
    {
        return kms;
    }

    /**
     * Returns the name of the encryption key created in KMS.
     */
    public String getEncryptionKeyName()
    {
        return ENCRYPTION_KEY_NAME;
    }

    /**
     * Returns the path to the encryption zone in HDFS.
     */
    public String getEncryptionZonePath()
    {
        return "/user/hive/encrypted";
    }
}
