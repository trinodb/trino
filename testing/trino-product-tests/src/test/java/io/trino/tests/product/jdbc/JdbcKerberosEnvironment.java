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
import io.trino.jdbc.TrinoDriver;
import io.trino.testing.containers.KerberosContainer;
import io.trino.testing.containers.TrinoTestImages;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
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
 *   <li>A standalone KDC container</li>
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
    private static final String TRUSTSTORE_RESOURCE = "jdbc/cert/truststore.jks";
    private static final String TRINO_PEM_RESOURCE = "jdbc/cert/trino.pem";
    private static final String TRINO_PEM_PATH = "/etc/trino/trino.pem";
    private static final String TRUSTSTORE_PASSWORD = "123456";
    private static final int HTTPS_PORT = 7778;

    // Kerberos realm and principals
    private static final String KERBEROS_REALM = KerberosContainer.REALM;
    private static final String CLIENT_PRINCIPAL = "trino-client/trino-master.docker.cluster";
    private static final String SERVER_PRINCIPAL = "trino-server/trino-master.docker.cluster";
    private static final String KERBEROS_PRINCIPAL = "trino-client/trino-master.docker.cluster@" + KERBEROS_REALM;
    private static final String KDC_CLIENT_KEYTAB_PATH = "/keytabs/trino-client.keytab";
    private static final String KDC_SERVER_KEYTAB_PATH = "/keytabs/trino-server.keytab";
    private static final String TRINO_SERVER_KEYTAB_PATH = "/etc/trino/trino-server.keytab";
    private static final String KERBEROS_SERVICE_NAME = "trino-server";
    private static final String KERBEROS_SERVICE_PRINCIPAL_PATTERN = "${SERVICE}@trino-master.docker.cluster";

    private Network network;
    private KerberosContainer kdc;
    private GenericContainer<?> trinoMaster;
    private Path truststorePath;
    private Path trinoPemPath;
    private Path keytabPath;
    private Path serverKeytabPath;
    private Path krb5ConfPath;

    @Override
    public void start()
    {
        if (trinoMaster != null && trinoMaster.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start standalone KDC and provision principals/keytabs.
        kdc = new KerberosContainer()
                .withNetwork(network)
                .withNetworkAliases(KerberosContainer.HOST_NAME)
                .withPrincipal(CLIENT_PRINCIPAL, KDC_CLIENT_KEYTAB_PATH)
                .withPrincipal(SERVER_PRINCIPAL, KDC_SERVER_KEYTAB_PATH);
        kdc.start();

        // Extract files/resources for client and server-side Kerberos + TLS setup.
        try {
            truststorePath = extractClasspathResource(TRUSTSTORE_RESOURCE);
            trinoPemPath = extractClasspathResource(TRINO_PEM_RESOURCE);
            keytabPath = writeTempFile("client-keytab-", ".keytab", kdc.getKeytab(KDC_CLIENT_KEYTAB_PATH));
            serverKeytabPath = writeTempFile("server-keytab-", ".keytab", kdc.getKeytab(KDC_SERVER_KEYTAB_PATH));
            krb5ConfPath = createExternalKrb5Conf();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to prepare Kerberos/TLS materials", e);
        }

        // Start Trino master with Kerberos configuration
        trinoMaster = new GenericContainer<>(DockerImageName.parse(TrinoTestImages.getDefaultTrinoImage()))
                .withNetwork(network)
                .withNetworkAliases("trino-master", "trino-master.docker.cluster")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("trino-master"))
                .withCreateContainerCmdModifier(cmd -> cmd.withDomainName("docker.cluster"))
                .withExposedPorts(HTTPS_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(trinoPemPath), TRINO_PEM_PATH)
                // /v1/info may respond before auth/system access-control are fully initialized.
                .waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        // Configure Trino for Kerberos authentication
        configureTrino(trinoMaster);
        copyHostFileToContainer(trinoMaster, serverKeytabPath, TRINO_SERVER_KEYTAB_PATH);

        trinoMaster.start();
    }

    private void configureTrino(GenericContainer<?> container)
    {
        // Config properties for Kerberos authentication
        String configProperties = """
                node.id=trino-coordinator
                node.environment=test
                node.internal-address-source=HOSTNAME
                coordinator=true
                node-scheduler.include-coordinator=true
                query.max-memory=1GB
                query.max-memory-per-node=1GB
                discovery.uri=https://trino-master:7778
                http-server.http.enabled=false
                http-server.https.enabled=true
                http-server.https.port=7778
                http-server.https.keystore.path=%s
                http.authentication.krb5.config=/etc/krb5.conf
                http-server.authentication.type=KERBEROS
                http-server.authentication.krb5.service-name=trino-server
                http-server.authentication.krb5.keytab=%s
                internal-communication.https.required=true
                internal-communication.shared-secret=internal-shared-secret
                internal-communication.https.keystore.path=%s
                http-server.log.enabled=false
                catalog.management=dynamic
                query.min-expire-age=1m
                task.info.max-age=1m
                """.formatted(TRINO_PEM_PATH, TRINO_SERVER_KEYTAB_PATH, TRINO_PEM_PATH);

        container.withCopyToContainer(
                Transferable.of(configProperties),
                "/etc/trino/config.properties");

        container.withCopyToContainer(
                Transferable.of("connector.name=tpch\n"),
                "/etc/trino/catalog/tpch.properties");

        container.withCopyToContainer(
                Transferable.of("connector.name=memory\n"),
                "/etc/trino/catalog/memory.properties");

        // krb5.conf with short ticket lifetime (80s) for testing expired tickets
        container.withCopyToContainer(
                Transferable.of(createInternalKrb5Conf()),
                "/etc/krb5.conf");
    }

    private Path extractClasspathResource(String resourcePath)
            throws IOException
    {
        String fileName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        Path tempFile = Files.createTempFile("jdbc-kerberos-", "-" + fileName);
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            Files.copy(input, tempFile, REPLACE_EXISTING);
        }
        return tempFile;
    }

    private static Path writeTempFile(String prefix, String suffix, byte[] data)
            throws IOException
    {
        Path tempFile = Files.createTempFile(prefix, suffix);
        Files.write(tempFile, data);
        return tempFile;
    }

    private static void copyHostFileToContainer(GenericContainer<?> container, Path hostFile, String containerPath)
    {
        try {
            // Ensure files are readable by the non-root trino user inside the container.
            container.withCopyToContainer(Transferable.of(Files.readAllBytes(hostFile), 0644), containerPath);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare container file: " + containerPath, e);
        }
    }

    private static String createInternalKrb5Conf()
    {
        return """
                [logging]
                 default = FILE:/var/log/krb5libs.log
                 kdc = FILE:/var/log/krb5kdc.log
                 admin_server = FILE:/var/log/kadmind.log

                [libdefaults]
                 default_realm = %s
                 dns_lookup_realm = false
                 dns_lookup_kdc = false
                 udp_preference_limit = 1
                 forwardable = true
                 allow_weak_crypto = true
                 ticket_lifetime = 80s

                [realms]
                 %s = {
                  kdc = %s:%d
                  admin_server = %s
                 }
                """.formatted(
                KERBEROS_REALM,
                KERBEROS_REALM,
                KerberosContainer.HOST_NAME,
                KerberosContainer.KDC_PORT,
                KerberosContainer.HOST_NAME);
    }

    private Path createExternalKrb5Conf()
            throws IOException
    {
        // Create krb5.conf that points to mapped KDC port on host.
        String krb5Conf = """
                [logging]
                 default = FILE:/var/log/krb5libs.log
                 kdc = FILE:/var/log/krb5kdc.log
                 admin_server = FILE:/var/log/kadmind.log

                [libdefaults]
                 default_realm = %s
                 dns_lookup_realm = false
                 dns_lookup_kdc = false
                 udp_preference_limit = 1
                 forwardable = true
                 allow_weak_crypto = true
                 ticket_lifetime = 80s

                [realms]
                 %s = {
                  kdc = %s:%d
                  admin_server = %s
                 }
                """.formatted(
                KERBEROS_REALM,
                KERBEROS_REALM,
                kdc.getHost(),
                kdc.getMappedPort(KerberosContainer.KDC_PORT),
                kdc.getHost());

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
                        "SSLVerification=CA&" +
                        "SSLTrustStorePath=%s&" +
                        "SSLTrustStorePassword=%s&" +
                        "KerberosRemoteServiceName=%s&" +
                        "KerberosUseCanonicalHostname=false&" +
                        "KerberosDelegation=true",
                trinoMaster.getHost(),
                trinoMaster.getMappedPort(HTTPS_PORT),
                truststorePath.toAbsolutePath(),
                TRUSTSTORE_PASSWORD,
                KERBEROS_SERVICE_NAME);

        Properties properties = new Properties();
        properties.put("KerberosServicePrincipalPattern", KERBEROS_SERVICE_PRINCIPAL_PATTERN);
        properties.put("KerberosConstrainedDelegation", credential);

        Connection connection = new TrinoDriver().connect(jdbcUrl, properties);
        if (connection == null) {
            throw new SQLException("Unable to create Trino connection for URL: " + jdbcUrl);
        }
        return connection;
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
        System.setProperty("sun.security.krb5.disableReferrals", "true");

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

        cleanupTempFile(trinoPemPath);
        trinoPemPath = null;

        cleanupTempFile(keytabPath);
        keytabPath = null;

        cleanupTempFile(serverKeytabPath);
        serverKeytabPath = null;

        cleanupTempFile(krb5ConfPath);
        krb5ConfPath = null;

        if (trinoMaster != null) {
            trinoMaster.close();
            trinoMaster = null;
        }
        if (kdc != null) {
            kdc.close();
            kdc = null;
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
