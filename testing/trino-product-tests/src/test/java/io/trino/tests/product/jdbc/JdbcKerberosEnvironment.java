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
package io.trino.tests.product.jdbc;

import com.google.common.collect.ImmutableSet;
import io.trino.testing.TestingProperties;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.ietf.jgss.GSSCredential.DEFAULT_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_ONLY;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

/**
 * Environment for JDBC Kerberos constrained delegation tests.
 * <p>
 * This environment configures:
 * <ul>
 *   <li>A Hadoop kerberized container with a KDC</li>
 *   <li>Trino server with Kerberos authentication and constrained delegation support</li>
 *   <li>Pre-configured keytabs and principals for testing</li>
 * </ul>
 * <p>
 * The environment uses a short ticket lifetime (80s) to allow testing of expired credentials.
 */
public class JdbcKerberosEnvironment
        extends ProductTestEnvironment
{
    private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";
    private static final String HADOOP_BASE_IMAGE = "ghcr.io/trinodb/testing/hdp3.1-hive";
    private static final String DEFAULT_TRINO_IMAGE = "trinodb/trino:latest";
    private static final String TRINO_IMAGE_PROPERTY = "trino.product-tests.image";
    private static final String KEYSTORE_PATH = "/docker/trino-product-tests/conf/trino/etc/docker.cluster.jks";
    private static final String KEYSTORE_PASSWORD = "123456";
    private static final int HTTPS_PORT = 7778;
    private static final int KDC_PORT = 88;

    // Kerberos realm and principals
    private static final String KERBEROS_REALM = "LABS.TERADATA.COM";
    private static final String KERBEROS_PRINCIPAL = "presto-client/presto-master.docker.cluster@" + KERBEROS_REALM;
    private static final String KERBEROS_KEYTAB_PATH = "/etc/trino/conf/presto-client.keytab";
    private static final String KERBEROS_SERVICE_NAME = "presto-server";

    private Network network;
    private GenericContainer<?> hadoopMaster;
    private GenericContainer<?> trinoMaster;
    private Path truststorePath;
    private Path keytabPath;
    private Path krb5ConfPath;

    @Override
    public void start()
    {
        if (trinoMaster != null && trinoMaster.isRunning()) {
            return; // Already started
        }

        String dockerImagesVersion = TestingProperties.getDockerImagesVersion();
        String kerberizedImage = HADOOP_BASE_IMAGE + "-kerberized:" + dockerImagesVersion;

        network = Network.newNetwork();

        // Start Hadoop master (KDC + Hadoop services)
        hadoopMaster = new GenericContainer<>(DockerImageName.parse(kerberizedImage))
                .withNetwork(network)
                .withNetworkAliases("hadoop-master")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("hadoop-master"))
                .withCreateContainerCmdModifier(cmd -> cmd.withDomainName("docker.cluster"))
                .withExposedPorts(KDC_PORT)
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(5));
        hadoopMaster.start();

        // Extract files from container
        try {
            truststorePath = extractFileFromContainer(hadoopMaster, KEYSTORE_PATH, "keystore-", ".jks");
            keytabPath = extractFileFromContainer(hadoopMaster, KERBEROS_KEYTAB_PATH, "keytab-", ".keytab");
            krb5ConfPath = createKrb5Conf();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to extract files from container", e);
        }

        // Start Trino master with Kerberos configuration
        trinoMaster = new GenericContainer<>(DockerImageName.parse(System.getProperty(TRINO_IMAGE_PROPERTY, DEFAULT_TRINO_IMAGE)))
                .withNetwork(network)
                .withNetworkAliases("presto-master", "presto-master.docker.cluster")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("presto-master"))
                .withCreateContainerCmdModifier(cmd -> cmd.withDomainName("docker.cluster"))
                .withExposedPorts(HTTPS_PORT)
                .waitingFor(Wait.forHttps("/v1/info")
                        .forPort(HTTPS_PORT)
                        .allowInsecure()
                        .withStartupTimeout(Duration.ofMinutes(2)));

        // Configure Trino for Kerberos authentication
        configureTrino(trinoMaster);

        trinoMaster.start();
    }

    private void configureTrino(GenericContainer<?> container)
    {
        // Config properties for Kerberos authentication
        String configProperties = """
                node.id=trino-coordinator
                node.environment=test
                node.internal-address-source=FQDN
                coordinator=true
                node-scheduler.include-coordinator=true
                query.max-memory=1GB
                query.max-memory-per-node=1GB
                discovery.uri=https://presto-master.docker.cluster:7778
                http-server.http.enabled=false
                http-server.https.enabled=true
                http-server.https.port=7778
                http-server.https.keystore.path=/docker/trino-product-tests/conf/trino/etc/docker.cluster.jks
                http-server.https.keystore.key=123456
                http.authentication.krb5.config=/etc/krb5.conf
                http-server.authentication.type=KERBEROS
                http-server.authentication.krb5.service-name=presto-server
                http-server.authentication.krb5.keytab=/etc/trino/conf/presto-server.keytab
                internal-communication.https.required=true
                internal-communication.shared-secret=internal-shared-secret
                internal-communication.https.keystore.path=/docker/trino-product-tests/conf/trino/etc/docker.cluster.jks
                internal-communication.https.keystore.key=123456
                http-server.log.enabled=false
                catalog.management=dynamic
                query.min-expire-age=1m
                task.info.max-age=1m
                """;

        container.withCopyToContainer(
                Transferable.of(configProperties),
                "/etc/trino/config.properties");

        // krb5.conf with short ticket lifetime (80s) for testing expired tickets
        String krb5Conf = """
                [logging]
                 default = FILE:/var/log/krb5libs.log
                 kdc = FILE:/var/log/krb5kdc.log
                 admin_server = FILE:/var/log/kadmind.log

                [libdefaults]
                 default_realm = LABS.TERADATA.COM
                 dns_lookup_realm = false
                 dns_lookup_kdc = false
                 forwardable = true
                 allow_weak_crypto = true
                 ticket_lifetime = 80s

                [realms]
                 LABS.TERADATA.COM = {
                  kdc = hadoop-master:88
                  admin_server = hadoop-master
                 }
                 OTHERLABS.TERADATA.COM = {
                  kdc = hadoop-master:89
                  admin_server = hadoop-master
                 }
                """;

        container.withCopyToContainer(
                Transferable.of(krb5Conf),
                "/etc/krb5.conf");
    }

    private Path extractFileFromContainer(GenericContainer<?> container, String containerPath, String prefix, String suffix)
            throws IOException
    {
        Path tempFile = Files.createTempFile(prefix, suffix);
        container.copyFileFromContainer(containerPath, tempFile.toString());
        return tempFile;
    }

    private Path createKrb5Conf()
            throws IOException
    {
        // Create krb5.conf that points to the exposed KDC port on localhost
        String krb5Conf = """
                [logging]
                 default = FILE:/var/log/krb5libs.log
                 kdc = FILE:/var/log/krb5kdc.log
                 admin_server = FILE:/var/log/kadmind.log

                [libdefaults]
                 default_realm = LABS.TERADATA.COM
                 dns_lookup_realm = false
                 dns_lookup_kdc = false
                 forwardable = true
                 allow_weak_crypto = true
                 ticket_lifetime = 80s

                [realms]
                 LABS.TERADATA.COM = {
                  kdc = %s:%d
                  admin_server = %s
                 }
                """.formatted(
                hadoopMaster.getHost(),
                hadoopMaster.getMappedPort(KDC_PORT),
                hadoopMaster.getHost());

        Path tempFile = Files.createTempFile("krb5-", ".conf");
        Files.writeString(tempFile, krb5Conf);
        return tempFile;
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        // For Kerberos, connections should be created with GSSCredential
        // This method provides a basic connection for checking connectivity
        throw new UnsupportedOperationException("Use createConnectionWithKerberosDelegation() for Kerberos tests");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        throw new UnsupportedOperationException("Use createConnectionWithKerberosDelegation() for Kerberos tests");
    }

    /**
     * Creates a JDBC connection using Kerberos constrained delegation.
     *
     * @param credential the GSS credential to use for authentication
     * @return a connection to Trino
     */
    public Connection createConnectionWithKerberosDelegation(GSSCredential credential)
            throws SQLException
    {
        String jdbcUrl = String.format(
                "jdbc:trino://%s:%d/tpch/tiny?" +
                        "SSL=true&" +
                        "SSLTrustStorePath=%s&" +
                        "SSLTrustStorePassword=%s&" +
                        "KerberosRemoteServiceName=%s&" +
                        "KerberosUseCanonicalHostname=false&" +
                        "KerberosDelegation=true",
                trinoMaster.getHost(),
                trinoMaster.getMappedPort(HTTPS_PORT),
                truststorePath.toAbsolutePath(),
                KEYSTORE_PASSWORD,
                KERBEROS_SERVICE_NAME);

        Properties properties = new Properties();
        properties.put("KerberosConstrainedDelegation", credential);

        return DriverManager.getConnection(jdbcUrl, properties);
    }

    /**
     * Creates a GSS credential for Kerberos authentication using the pre-configured keytab.
     */
    public GSSCredential createGssCredential()
    {
        Subject authenticatedSubject = authenticateWithKerberos();
        Principal clientPrincipal = authenticatedSubject.getPrincipals().iterator().next();

        return Subject.callAs(authenticatedSubject, () -> {
            GSSManager gssManager = GSSManager.getInstance();
            return gssManager.createCredential(
                    gssManager.createName(clientPrincipal.getName(), NT_USER_NAME),
                    DEFAULT_LIFETIME,
                    new Oid(KERBEROS_OID),
                    INITIATE_ONLY);
        });
    }

    /**
     * Authenticates using Kerberos with the configured principal and keytab.
     */
    private Subject authenticateWithKerberos()
    {
        requireNonNull(keytabPath, "keytabPath is null");
        requireNonNull(krb5ConfPath, "krb5ConfPath is null");

        // Set krb5.conf path
        System.setProperty("java.security.krb5.conf", krb5ConfPath.toAbsolutePath().toString());

        KerberosPrincipal principal = new KerberosPrincipal(KERBEROS_PRINCIPAL);
        Configuration configuration = createKerberosConfiguration(KERBEROS_PRINCIPAL, keytabPath.toAbsolutePath().toString());

        Subject subject = new Subject(false, ImmutableSet.of(principal), emptySet(), emptySet());
        try {
            LoginContext loginContext = new LoginContext("", subject, null, configuration);
            loginContext.login();
            return loginContext.getSubject();
        }
        catch (LoginException e) {
            throw new RuntimeException("Kerberos authentication failed", e);
        }
    }

    private static Configuration createKerberosConfiguration(String principal, String keytab)
    {
        Map<String, String> loginOptions = Map.of(
                "useKeyTab", "true",
                "principal", principal,
                "keyTab", keytab,
                "storeKey", "true",
                "doNotPrompt", "true",
                "isInitiator", "true");

        return new Configuration()
        {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name)
            {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(
                                "com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                loginOptions)
                };
            }
        };
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trinoMaster != null
                ? String.format("jdbc:trino://%s:%d", trinoMaster.getHost(), trinoMaster.getMappedPort(HTTPS_PORT))
                : null;
    }

    @Override
    public boolean isRunning()
    {
        return trinoMaster != null && trinoMaster.isRunning();
    }

    @Override
    protected void doClose()
    {
        cleanupTempFile(truststorePath);
        truststorePath = null;

        cleanupTempFile(keytabPath);
        keytabPath = null;

        cleanupTempFile(krb5ConfPath);
        krb5ConfPath = null;

        if (trinoMaster != null) {
            trinoMaster.close();
            trinoMaster = null;
        }
        if (hadoopMaster != null) {
            hadoopMaster.close();
            hadoopMaster = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private void cleanupTempFile(Path path)
    {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            }
            catch (IOException e) {
                // Ignore cleanup errors
            }
        }
    }

    public String getKerberosPrincipal()
    {
        return KERBEROS_PRINCIPAL;
    }

    public String getKeytabPath()
    {
        return keytabPath != null ? keytabPath.toAbsolutePath().toString() : null;
    }
}
