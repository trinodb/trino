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
package io.trino.tests.product.ldap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.TestingProperties;
import io.trino.testing.containers.TrinoTestImages;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.tests.product.utils.HostMappingDnsResolver;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.AttributeInUseException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.ModificationItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Abstract base environment for LDAP authentication tests.
 * <p>
 * This environment configures:
 * <ul>
 *   <li>OpenLDAP server with SSL (LDAPS on port 636)</li>
 *   <li>Trino with HTTPS and LDAP password authentication</li>
 *   <li>Pre-configured LDAP users and groups for testing</li>
 * </ul>
 * <p>
 * Subclasses configure specific authentication settings.
 */
public abstract class LdapEnvironment
        extends ProductTestEnvironment
{
    protected static final String OPENLDAP_IMAGE = "ghcr.io/trinodb/testing/almalinux9-oj17-openldap:" + TestingProperties.getDockerImagesVersion();
    protected static final String OPENLDAP_REFERRALS_IMAGE = "ghcr.io/trinodb/testing/almalinux9-oj17-openldap-referrals:" + TestingProperties.getDockerImagesVersion();
    protected static final int LDAP_PORT = 389;
    protected static final int LDAPS_PORT = 636;
    protected static final int HTTPS_PORT = 8443;

    protected static final String LDAP_PASSWORD = "LDAPPass123";
    protected static final String BASE_DN = "dc=trino,dc=testldap,dc=com";
    protected static final String AMERICA_ORG_DN = "ou=America," + BASE_DN;
    protected static final String ASIA_ORG_DN = "ou=Asia," + BASE_DN;
    protected static final String EUROPE_ORG_DN = "ou=Europe," + BASE_DN;

    private static final String CLI_CONTAINER_PATH = "/docker/trino-cli";
    // Path where certs are stored inside the Trino container
    private static final String TRINO_CERTS_DIR = "/etc/trino/certs";
    private static final String TRINO_HTTPS_CERT_RESOURCE = "oauth2/cert/trino.pem";
    private static final String TRINO_HTTPS_CERT_FILE = "trino-server.pem";
    protected static final String TRINO_LDAP_CLIENT_CERT_FILE = "trino-client-for-ldap.pem";
    private static final String TRINO_TRUSTSTORE_RESOURCE = "oauth2/cert/truststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "123456";
    private static final String TRINO_HOST = "trino";

    private Network network;
    protected GenericContainer<?> ldapServer;
    protected GenericContainer<?> trinoContainer;
    private Path truststorePath;
    private Path cliJarPath;
    private Path tempCertsDir;

    @Override
    public void start()
    {
        if (trinoContainer != null && trinoContainer.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        // Start OpenLDAP server with both SSL and non-SSL ports
        ldapServer = new GenericContainer<>(DockerImageName.parse(getOpenLdapImage()))
                .withNetwork(network)
                .withNetworkAliases("ldapserver")
                .withExposedPorts(LDAP_PORT, LDAPS_PORT)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forLogMessage(".*success: slapd entered RUNNING state.*", 1))
                .withStartupTimeout(Duration.ofMinutes(5));
        ldapServer.start();

        // Extract certificates from LDAP container to host
        try {
            tempCertsDir = Files.createTempDirectory("ldap-certs-");
            generateLdapClientCertificate();
            extractCertFromContainer("openldap-certificate.pem");
            extractClasspathResource(TRINO_HTTPS_CERT_RESOURCE, TRINO_HTTPS_CERT_FILE);
            extractClasspathResource(TRINO_TRUSTSTORE_RESOURCE, "cacerts.jks");
            truststorePath = tempCertsDir.resolve("cacerts.jks");
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to extract certificates from LDAP container", e);
        }

        // Create LDAP users and groups using the non-SSL port
        try {
            setupLdapUsersAndGroups();
        }
        catch (NamingException e) {
            throw new RuntimeException("Failed to setup LDAP users and groups", e);
        }

        // Find CLI jar for CLI tests
        cliJarPath = findCliJar();

        // Start Trino with HTTPS and LDAP authentication using the standard Trino image
        trinoContainer = new GenericContainer<>(DockerImageName.parse(TrinoTestImages.getDefaultTrinoImage()))
                .withNetwork(network)
                .withNetworkAliases(TRINO_HOST)
                .withExposedPorts(HTTPS_PORT);

        // Copy certificates from the LDAP container into the Trino container.
        // Host bind mounts are brittle on Linux CI because the extracted temp directory may not be
        // readable by the Trino container user even when the files exist.
        copyCertToTrinoContainer(trinoContainer, "cacerts.jks");
        copyCertToTrinoContainer(trinoContainer, TRINO_HTTPS_CERT_FILE);
        copyCertToTrinoContainer(trinoContainer, TRINO_LDAP_CLIENT_CERT_FILE);
        copyCertToTrinoContainer(trinoContainer, "openldap-certificate.pem");

        // Copy CLI jar into container with executable permissions. Artifact downloads on CI do not
        // preserve the executable bit reliably, which breaks direct execution from bind mounts.
        if (cliJarPath != null) {
            trinoContainer.withCopyFileToContainer(MountableFile.forHostPath(cliJarPath, 0755), CLI_CONTAINER_PATH);
        }

        // Configure Trino for HTTPS and LDAP (subclass-specific)
        configureTrino(trinoContainer);

        // Wait for Trino to fully start including password authenticator loading
        // Using log message wait because the HTTPS endpoint (/v1/info) becomes available
        // before password authenticators are fully loaded
        trinoContainer.waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));

        trinoContainer.start();
    }

    /**
     * Subclasses override this to provide specific Trino configuration.
     */
    protected abstract void configureTrino(GenericContainer<?> container);

    protected String getOpenLdapImage()
    {
        return OPENLDAP_IMAGE;
    }

    /**
     * Returns the base config.properties content for HTTPS.
     */
    protected String getBaseConfigProperties()
    {
        return """
                node.id=trino-coordinator
                node.environment=test
                coordinator=true
                node-scheduler.include-coordinator=true
                query.max-memory=1GB
                query.max-memory-per-node=1GB
                discovery.uri=http://localhost:8080
                http-server.http.port=8080
                http-server.https.port=8443
                http-server.https.enabled=true
                http-server.https.keystore.path=%s/%s
                http-server.authentication.type=PASSWORD
                internal-communication.shared-secret=internal-shared-secret
                catalog.management=dynamic
                """.formatted(TRINO_CERTS_DIR, TRINO_HTTPS_CERT_FILE);
    }

    /**
     * Returns the base password-authenticator.properties content for LDAP.
     */
    protected String getLdapPasswordAuthenticatorProperties()
    {
        return """
                password-authenticator.name=ldap
                ldap.url=ldaps://ldapserver:636
                ldap.ssl.keystore.path=%s/%s
                ldap.ssl.truststore.path=%s/openldap-certificate.pem
                ldap.user-bind-pattern=uid=${USER},ou=America,dc=trino,dc=testldap,dc=com:uid=${USER},ou=Asia,dc=trino,dc=testldap,dc=com
                ldap.user-base-dn=dc=trino,dc=testldap,dc=com
                ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=cn=DefaultGroup,ou=America,dc=trino,dc=testldap,dc=com))
                """.formatted(TRINO_CERTS_DIR, TRINO_LDAP_CLIENT_CERT_FILE, TRINO_CERTS_DIR);
    }

    public String expectedUserNotInGroupMessage(String user)
    {
        return format("Authentication failed: Access Denied: User [%s] not a member of an authorized group", user);
    }

    public String expectedWrongLdapPasswordMessage()
    {
        return "Authentication failed: Access Denied: Invalid credentials";
    }

    public String expectedWrongLdapUserMessage()
    {
        return "Authentication failed: Access Denied: Invalid credentials";
    }

    public String expectedFailedBindMessage()
    {
        return "Authentication failed: Access Denied: Invalid credentials";
    }

    private void extractCertFromContainer(String certFileName)
            throws IOException
    {
        Path targetPath = tempCertsDir.resolve(certFileName);
        ldapServer.copyFileFromContainer("/etc/openldap/certs/" + certFileName, targetPath.toString());
    }

    private void generateLdapClientCertificate()
            throws IOException
    {
        // TODO: Burn this into the LDAP test image once the JUnit product-test migration settles.
        String command = """
                set -euo pipefail
                tmpdir=$(mktemp -d)
                openssl req -new -newkey rsa:4096 -nodes \
                    -keyout "$tmpdir/client.key" \
                    -out "$tmpdir/client.csr" \
                    -subj "/CN=trino/OU=TEST/O=TRINO/L=Chennai/ST=TN/C=IN"
                cert="/tmp/%s"
                signed_cert="$tmpdir/client.crt"
                openssl x509 -req -sha256 \
                    -in "$tmpdir/client.csr" \
                    -CA /etc/openldap/certs/openldap-certificate.pem \
                    -CAkey /etc/openldap/certs/private.pem \
                    -CAcreateserial \
                    -CAserial "$tmpdir/client.srl" \
                    -out "$signed_cert" \
                    -days 36500
                cat "$tmpdir/client.key" "$signed_cert" > "$cert"
                rm -rf "$tmpdir"
                """.formatted(TRINO_LDAP_CLIENT_CERT_FILE);
        try {
            ExecResult result = ldapServer.execInContainer("sh", "-lc", command);
            if (result.getExitCode() != 0) {
                throw new IOException("Failed to generate LDAP client certificate: " + result.getStderr());
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while generating LDAP client certificate", e);
        }

        ldapServer.copyFileFromContainer("/tmp/" + TRINO_LDAP_CLIENT_CERT_FILE, tempCertsDir.resolve(TRINO_LDAP_CLIENT_CERT_FILE).toString());
    }

    private void extractClasspathResource(String resourcePath, String outputFileName)
            throws IOException
    {
        try (var input = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            Files.copy(input, tempCertsDir.resolve(outputFileName));
        }
    }

    private void copyCertToTrinoContainer(GenericContainer<?> container, String certFileName)
    {
        container.withCopyFileToContainer(
                MountableFile.forHostPath(tempCertsDir.resolve(certFileName)),
                TRINO_CERTS_DIR + "/" + certFileName);
    }

    @SuppressWarnings("BanJNDI")
    private void setupLdapUsersAndGroups()
            throws NamingException
    {
        DirContext context = createLdapContext();
        try {
            // Create organizations
            createOrganization(context, "America", BASE_DN);
            createOrganization(context, "Asia", BASE_DN);
            createOrganization(context, "Europe", BASE_DN);

            // Create users in Asia org (default)
            createUser(context, "DefaultGroupUser", ASIA_ORG_DN, LDAP_PASSWORD);
            createUser(context, "ChildGroupUser", ASIA_ORG_DN, LDAP_PASSWORD);
            createUser(context, "ParentGroupUser", ASIA_ORG_DN, LDAP_PASSWORD);
            createUser(context, "OrphanUser", ASIA_ORG_DN, LDAP_PASSWORD);
            createUser(context, "User WithSpecialPwd", ASIA_ORG_DN, "LDAP:Pass ~!@#$%^&*()_+{}|:\"<>?/.,';\\][=-`");
            createUser(context, "UserInMultipleGroups", ASIA_ORG_DN, LDAP_PASSWORD);

            // Create users in specific orgs
            createUser(context, "AmericanUser", AMERICA_ORG_DN, LDAP_PASSWORD);
            createUser(context, "EuropeUser", EUROPE_ORG_DN, LDAP_PASSWORD);

            // Legacy coverage used nested groups: DefaultGroup contains ChildGroup and ParentGroup
            // contains DefaultGroup plus a directly authorized user.
            String childGroupDn = createGroup(context, "ChildGroup", AMERICA_ORG_DN, "uid=ChildGroupUser," + ASIA_ORG_DN);
            String defaultGroupDn = createGroup(context, "DefaultGroup", AMERICA_ORG_DN, "uid=DefaultGroupUser," + ASIA_ORG_DN);
            String parentGroupDn = createGroup(context, "ParentGroup", AMERICA_ORG_DN, "uid=ParentGroupUser," + ASIA_ORG_DN);

            // Add additional users to default group
            addMemberToGroup(context, defaultGroupDn, childGroupDn);
            addUserToGroup(context, defaultGroupDn, "uid=User WithSpecialPwd," + ASIA_ORG_DN);
            addUserToGroup(context, defaultGroupDn, "uid=UserInMultipleGroups," + ASIA_ORG_DN);
            addUserToGroup(context, defaultGroupDn, "uid=AmericanUser," + AMERICA_ORG_DN);
            addUserToGroup(context, defaultGroupDn, "uid=EuropeUser," + EUROPE_ORG_DN);
            addMemberToGroup(context, parentGroupDn, defaultGroupDn);
            addUserToGroup(context, parentGroupDn, "uid=UserInMultipleGroups," + ASIA_ORG_DN);
        }
        finally {
            context.close();
        }
    }

    @SuppressWarnings("BanJNDI")
    private DirContext createLdapContext()
    {
        String ldapUrl = format("ldap://%s:%d", ldapServer.getHost(), ldapServer.getMappedPort(LDAP_PORT));
        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, ldapUrl)
                .put(Context.SECURITY_AUTHENTICATION, "simple")
                .put(Context.SECURITY_PRINCIPAL, "cn=admin," + BASE_DN)
                .put(Context.SECURITY_CREDENTIALS, "admin")
                .buildOrThrow();
        try {
            Properties properties = new Properties();
            properties.putAll(environment);
            return new InitialDirContext(properties);
        }
        catch (NamingException e) {
            throw new RuntimeException("Connection to LDAP server failed", e);
        }
    }

    @SuppressWarnings("BanJNDI")
    private void createOrganization(DirContext context, String name, String baseDn)
            throws NamingException
    {
        Attributes attrs = new BasicAttributes();
        Attribute objectClass = new BasicAttribute("objectClass");
        objectClass.add("top");
        objectClass.add("organizationalUnit");
        attrs.put(objectClass);
        attrs.put("ou", name);
        try {
            context.createSubcontext("ou=" + name + "," + baseDn, attrs);
        }
        catch (NameAlreadyBoundException _) {
            // Organization already exists in the LDAP directory - ignore
        }
    }

    @SuppressWarnings("BanJNDI")
    private void createUser(DirContext context, String userName, String orgDn, String password)
            throws NamingException
    {
        Attributes attrs = new BasicAttributes();
        Attribute objectClass = new BasicAttribute("objectClass");
        objectClass.add("person");
        objectClass.add("inetOrgPerson");
        attrs.put(objectClass);
        attrs.put("cn", userName);
        attrs.put("sn", userName);
        attrs.put("userPassword", password);
        try {
            context.createSubcontext("uid=" + userName + "," + orgDn, attrs);
        }
        catch (NameAlreadyBoundException _) {
            // User already exists in the LDAP directory - ignore
        }
    }

    @SuppressWarnings("BanJNDI")
    private String createGroup(DirContext context, String groupName, String orgDn, String initialMemberDn)
            throws NamingException
    {
        String groupDn = "cn=" + groupName + "," + orgDn;
        Attributes attrs = new BasicAttributes();
        Attribute objectClass = new BasicAttribute("objectClass");
        objectClass.add("groupOfNames");
        attrs.put(objectClass);
        attrs.put("cn", groupName);
        attrs.put("member", initialMemberDn);
        try {
            context.createSubcontext(groupDn, attrs);
        }
        catch (NameAlreadyBoundException _) {
            // Group already exists in the LDAP directory - ignore
        }
        return groupDn;
    }

    @SuppressWarnings("BanJNDI")
    private void addUserToGroup(DirContext context, String groupDn, String userDn)
            throws NamingException
    {
        addMemberToGroup(context, groupDn, userDn);
    }

    @SuppressWarnings("BanJNDI")
    private void addMemberToGroup(DirContext context, String groupDn, String memberDn)
            throws NamingException
    {
        ModificationItem[] mods = new ModificationItem[] {
                new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("member", memberDn))
        };
        try {
            context.modifyAttributes(groupDn, mods);
        }
        catch (AttributeInUseException _) {
            // Member already in group - ignore
        }
    }

    // ==================== ProductTestEnvironment Methods ====================

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return createStrictJdbcConnection(getDefaultUser(), getDefaultPassword());
    }

    /**
     * Creates a JDBC connection to Trino using LDAP authentication.
     */
    @Override
    public Connection createTrinoConnection(String user)
    {
        throw new UnsupportedOperationException("LDAP connections require both user and password; use createTrinoConnection(String, String)");
    }

    public Connection createTrinoConnection(String user, String password)
            throws SQLException
    {
        return createStrictJdbcConnection(user, password);
    }

    private Connection createStrictJdbcConnection(String user, String password)
            throws SQLException
    {
        String url = format("jdbc:trino://%s:%d?SSL=true&SSLTrustStorePath=%s&SSLTrustStorePassword=%s",
                getTrinoHostName(),
                trinoContainer.getMappedPort(HTTPS_PORT),
                getTruststorePath(),
                getTruststorePassword());

        Properties properties = new Properties();
        properties.setProperty("user", user);
        if (password != null) {
            properties.setProperty("password", password);
        }
        properties.setProperty("dnsResolver", HostMappingDnsResolver.class.getName());
        properties.setProperty("dnsResolverContext", "%s=%s".formatted(
                getTrinoHostName(), trinoContainer.getHost()));

        return DriverManager.getConnection(url, properties);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trinoContainer != null
                ? format("jdbc:trino://%s:%d", trinoContainer.getHost(), trinoContainer.getMappedPort(HTTPS_PORT))
                : null;
    }

    @Override
    public boolean isRunning()
    {
        return trinoContainer != null && trinoContainer.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (truststorePath != null) {
            truststorePath = null;
        }
        if (trinoContainer != null) {
            trinoContainer.close();
            trinoContainer = null;
        }
        if (ldapServer != null) {
            ldapServer.close();
            ldapServer = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
        if (tempCertsDir != null) {
            try {
                // Clean up temp cert files
                try (Stream<Path> stream = Files.walk(tempCertsDir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                }
                                catch (IOException e) {
                                    // Ignore cleanup errors
                                }
                            });
                }
            }
            catch (IOException e) {
                // Ignore cleanup errors
            }
            tempCertsDir = null;
        }
    }

    // ==================== CLI Execution Methods ====================

    /**
     * Executes a CLI command with the given user credentials.
     */
    public ExecResult executeCli(String user, String password, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add(format("https://%s:%d", getTrinoHostName(), HTTPS_PORT));
        command.add("--truststore-path");
        command.add(TRINO_CERTS_DIR + "/cacerts.jks");
        command.add("--truststore-password");
        command.add(TRUSTSTORE_PASSWORD);
        command.add("--user");
        command.add(user);
        command.add("--password");

        for (String arg : args) {
            command.add(arg);
        }

        // Set TRINO_PASSWORD environment variable via bash
        // Use escapeForBash for the password value (wraps in single quotes)
        return trinoContainer.execInContainer(
                ImmutableList.<String>builder()
                        .add("/bin/bash", "-c")
                        .add("TRINO_PASSWORD=" + escapeForBash(password) + " " + String.join(" ", escapeCommand(command)))
                        .build()
                        .toArray(new String[0]));
    }

    /**
     * Executes a CLI command without password (for testing auth failure).
     */
    public ExecResult executeCliWithoutPassword(String user, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add("https://localhost:8443");
        command.add("--insecure");
        command.add("--user");
        command.add(user);

        for (String arg : args) {
            command.add(arg);
        }

        return trinoContainer.execInContainer(command.toArray(new String[0]));
    }

    /**
     * Executes a CLI command using HTTP URL (for testing SSL requirement).
     */
    public ExecResult executeCliWithHttpUrl(String user, String password, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add("http://localhost:8080");  // HTTP instead of HTTPS
        command.add("--user");
        command.add(user);
        command.add("--password");

        for (String arg : args) {
            command.add(arg);
        }

        // Set TRINO_PASSWORD environment variable
        return trinoContainer.execInContainer(
                ImmutableList.<String>builder()
                        .add("/bin/bash", "-c")
                        .add("TRINO_PASSWORD=" + escapeForBash(password) + " " + String.join(" ", escapeCommand(command)))
                        .build()
                        .toArray(new String[0]));
    }

    /**
     * Executes a CLI command with wrong truststore password (for testing SSL setup error).
     */
    public ExecResult executeCliWithWrongTruststorePassword(String user, String password, String... args)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(CLI_CONTAINER_PATH);
        command.add("--server");
        command.add("https://localhost:8443");
        command.add("--truststore-path");
        command.add(TRINO_CERTS_DIR + "/cacerts.jks");
        command.add("--truststore-password");
        command.add("wrong_password");  // Wrong password
        command.add("--user");
        command.add(user);
        command.add("--password");

        for (String arg : args) {
            command.add(arg);
        }

        // Set TRINO_PASSWORD environment variable
        return trinoContainer.execInContainer(
                ImmutableList.<String>builder()
                        .add("/bin/bash", "-c")
                        .add("TRINO_PASSWORD=" + escapeForBash(password) + " " + String.join(" ", escapeCommand(command)))
                        .build()
                        .toArray(new String[0]));
    }

    /**
     * Returns the Trino container for advanced CLI operations.
     */
    public GenericContainer<?> getTrinoContainer()
    {
        return trinoContainer;
    }

    private List<String> escapeCommand(List<String> command)
    {
        return command.stream()
                .map(this::escapeForBash)
                .toList();
    }

    private String escapeForBash(String value)
    {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    private static Path findCliJar()
    {
        String version = TestingProperties.getProjectVersion();
        Path projectRoot = findProjectRoot();
        if (projectRoot == null) {
            return null;
        }
        Path cliJar = projectRoot.resolve("client/trino-cli/target/trino-cli-" + version + "-executable.jar");
        if (Files.exists(cliJar)) {
            return cliJar;
        }
        // Fallback for local layouts where only the plain jar is available.
        Path plainCliJar = projectRoot.resolve("client/trino-cli/target/trino-cli-" + version + ".jar");
        if (Files.exists(plainCliJar)) {
            return plainCliJar;
        }
        return null;
    }

    private static Path findProjectRoot()
    {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path clientDir = current.resolve("client");
            Path cliPom = clientDir.resolve("trino-cli/pom.xml");
            if (Files.exists(cliPom)) {
                return current;
            }
            current = current.getParent();
        }
        return null;
    }

    // ==================== Getter Methods ====================

    public String getTrinoServerAddress()
    {
        return format("https://%s:%d", trinoContainer.getHost(), trinoContainer.getMappedPort(HTTPS_PORT));
    }

    public String getTrinoHostName()
    {
        return TRINO_HOST;
    }

    public String getTrinoHost()
    {
        return trinoContainer.getHost();
    }

    public int getTrinoPort()
    {
        return trinoContainer.getMappedPort(HTTPS_PORT);
    }

    public String getTruststorePath()
    {
        return truststorePath.toAbsolutePath().toString();
    }

    public String getTruststorePassword()
    {
        return TRUSTSTORE_PASSWORD;
    }

    public String getDefaultUser()
    {
        return "DefaultGroupUser";
    }

    public String getDefaultPassword()
    {
        return LDAP_PASSWORD;
    }

    public String getLdapPassword()
    {
        return LDAP_PASSWORD;
    }

    public String getChildGroupUser()
    {
        return "ChildGroupUser";
    }

    public String getParentGroupUser()
    {
        return "ParentGroupUser";
    }

    public String getOrphanUser()
    {
        return "OrphanUser";
    }

    public String getSpecialUser()
    {
        return "User WithSpecialPwd";
    }

    public String getSpecialUserPassword()
    {
        return "LDAP:Pass ~!@#$%^&*()_+{}|:\"<>?/.,';\\][=-`";
    }

    public String getUserInMultipleGroups()
    {
        return "UserInMultipleGroups";
    }

    public String getUserInAmerica()
    {
        return "AmericanUser";
    }

    public String getUserInEurope()
    {
        return "EuropeUser";
    }
}
