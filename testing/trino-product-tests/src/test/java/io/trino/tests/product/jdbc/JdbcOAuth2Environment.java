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

import io.trino.testing.containers.TrinoTestImages;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.tests.product.utils.HostMappingDnsResolver;
import okhttp3.Dns;
import okhttp3.HttpUrl;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.CookieManager;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Abstract base environment for OAuth2 authentication tests.
 * <p>
 * This environment configures:
 * <ul>
 *   <li>Ory Hydra OAuth2 provider with PostgreSQL backend</li>
 *   <li>A simple Python login/consent server</li>
 *   <li>Trino with HTTPS and OAuth2 authentication</li>
 * </ul>
 * <p>
 * Subclasses configure specific OAuth2 settings (with/without refresh tokens).
 */
public abstract class JdbcOAuth2Environment
        extends ProductTestEnvironment
{
    protected static final String HYDRA_IMAGE = "oryd/hydra:v1.11.10";
    protected static final int TTL_ACCESS_TOKEN_IN_SECONDS = 5;
    protected static final int TTL_REFRESH_TOKEN_IN_SECONDS = 15;
    protected static final int HTTPS_PORT = 7778;

    protected static final String CLIENT_ID = "trinodb_client_id";
    protected static final String CLIENT_SECRET = "trinodb_client_secret";
    private static final String TRINO_HOST = "trino";
    private static final String HYDRA_HOST = "hydra";
    private static final String HYDRA_CONSENT_HOST = "hydra-consent";

    protected Network network;
    protected PostgreSQLContainer<?> hydraDb;
    protected GenericContainer<?> hydraMigration;
    protected GenericContainer<?> hydraConsent;
    protected GenericContainer<?> hydra;
    protected GenericContainer<?> hydraClientPreparation;
    protected GenericContainer<?> trinoContainer;
    protected Path truststorePath;
    private OkHttpClient httpClient;

    @Override
    public void start()
    {
        if (trinoContainer != null && trinoContainer.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        // Start Hydra's PostgreSQL database
        hydraDb = new PostgreSQLContainer<>(DockerImageName.parse("postgres:14.0"))
                .withNetwork(network)
                .withNetworkAliases("hydra-db")
                .withDatabaseName("hydra")
                .withUsername("hydra")
                .withPassword("mysecretpassword");
        hydraDb.start();

        // Run Hydra database migrations
        hydraMigration = new GenericContainer<>(DockerImageName.parse(HYDRA_IMAGE))
                .withNetwork(network)
                .withCommand("migrate", "sql", "--yes", getDsn())
                .dependsOn(hydraDb)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy());
        hydraMigration.start();

        // Start the login and consent server
        hydraConsent = new GenericContainer<>(DockerImageName.parse("python:3.10.1-alpine"))
                .withNetwork(network)
                .withNetworkAliases("hydra-consent")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("oauth2/login_and_consent_server.py"),
                        "/login_and_consent_server.py")
                .withCommand("python", "/login_and_consent_server.py")
                .withExposedPorts(3000)
                .waitingFor(Wait.forHttp("/healthz").forPort(3000).forStatusCode(200));
        hydraConsent.start();

        // Extract certificates to temporary files
        try {
            truststorePath = extractClasspathResource("oauth2/cert/truststore.jks");
            Path hydraPemPath = extractClasspathResource("oauth2/cert/hydra.pem");
            Path trinoPemPath = extractClasspathResource("oauth2/cert/trino.pem");

            startAdditionalServices(hydraPemPath, trinoPemPath);

            // Start Hydra OAuth2 server
            hydra = new GenericContainer<>(DockerImageName.parse(HYDRA_IMAGE))
                    .withNetwork(network)
                    .withNetworkAliases("hydra")
                    .withEnv("LOG_LEVEL", "debug")
                    .withEnv("LOG_LEAK_SENSITIVE_VALUES", "true")
                    .withEnv("OAUTH2_EXPOSE_INTERNAL_ERRORS", "1")
                    .withEnv("GODEBUG", "http2debug=1")
                    .withEnv("DSN", getDsn())
                    .withEnv("URLS_SELF_ISSUER", "https://hydra:4444/")
                    .withEnv("URLS_CONSENT", "http://hydra-consent:3000/consent")
                    .withEnv("URLS_LOGIN", "http://hydra-consent:3000/login")
                    .withEnv("SERVE_TLS_KEY_PATH", "/tmp/certs/hydra.pem")
                    .withEnv("SERVE_TLS_CERT_PATH", "/tmp/certs/hydra.pem")
                    .withEnv("STRATEGIES_ACCESS_TOKEN", "jwt")
                    .withEnv("TTL_ACCESS_TOKEN", TTL_ACCESS_TOKEN_IN_SECONDS + "s")
                    .withEnv("TTL_REFRESH_TOKEN", TTL_REFRESH_TOKEN_IN_SECONDS + "s")
                    .withEnv("OAUTH2_ALLOWED_TOP_LEVEL_CLAIMS", "groups")
                    .withCommand("serve", "all")
                    .withCopyFileToContainer(MountableFile.forHostPath(hydraPemPath), "/tmp/certs/hydra.pem")
                    .withExposedPorts(4444, 4445)
                    .dependsOn(hydraDb, hydraMigration, hydraConsent)
                    .waitingFor(new WaitAllStrategy()
                            .withStrategy(Wait.forLogMessage(".*Setting up http server on :4444.*", 1))
                            .withStrategy(Wait.forLogMessage(".*Setting up http server on :4445.*", 1)));
            hydra.start();

            // Start Trino with OAuth2 authentication using GenericContainer
            // We use GenericContainer instead of TrinoContainer because we need custom HTTPS config
            trinoContainer = new FixedHttpsPortTrinoContainer(DockerImageName.parse(TrinoTestImages.getDefaultTrinoImage()))
                    .withNetwork(network)
                    .withNetworkAliases(TRINO_HOST)
                    .withExposedPorts(8080, HTTPS_PORT)
                    .withCopyFileToContainer(
                            MountableFile.forHostPath(trinoPemPath),
                            "/etc/trino/trino.pem")
                    .withCopyFileToContainer(
                            MountableFile.forHostPath(hydraPemPath),
                            "/etc/trino/hydra.pem");

            // Configure Trino for OAuth2 (subclass-specific)
            configureTrino(trinoContainer);

            // OAuth2Client.load() runs late in server startup, after the HTTP endpoints become
            // reachable. Wait for full startup so the first JDBC auth request does not race it.
            trinoContainer.waitingFor(new WaitAllStrategy()
                    .withStrategy(Wait.forHttp("/v1/info").forPort(8080).forStatusCode(200))
                    .withStrategy(Wait.forHttps("/v1/info").forPort(HTTPS_PORT).allowInsecure())
                    .withStrategy(Wait.forLogMessage(".*SERVER STARTED.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(2)));

            hydraClientPreparation = new GenericContainer<>(DockerImageName.parse(HYDRA_IMAGE))
                    .withNetwork(network)
                    .withCommand("clients", "create",
                            "--endpoint", "https://hydra:4445",
                            "--skip-tls-verify",
                            "--id", CLIENT_ID,
                            "--secret", CLIENT_SECRET,
                            "--audience", CLIENT_ID,
                            "-g", "authorization_code,refresh_token,client_credentials",
                            "-r", "token,code,id_token",
                            "--scope", "openid,offline",
                            "--token-endpoint-auth-method", "client_secret_basic",
                            "--callbacks", "https://" + TRINO_HOST + ":" + HTTPS_PORT + "/oauth2/callback,https://localhost:" + HTTPS_PORT + "/oauth2/callback")
                    .dependsOn(hydra)
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy());
            hydraClientPreparation.start();

            trinoContainer.start();

            // Setup HTTP client with trust store
            setupHttpClient();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to setup OAuth2 environment", e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to setup OAuth2 environment", e);
        }
    }

    private String getDsn()
    {
        return "postgres://hydra:mysecretpassword@hydra-db:5432/hydra?sslmode=disable";
    }

    protected void startAdditionalServices(Path hydraPemPath, Path trinoPemPath)
            throws Exception
    {
    }

    protected void stopAdditionalServices()
            throws Exception
    {
    }

    protected Path extractClasspathResource(String resourcePath)
            throws IOException
    {
        String fileName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        Path tempFile = Files.createTempFile("oauth2-", "-" + fileName);
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            Files.copy(is, tempFile, REPLACE_EXISTING);
        }
        return tempFile;
    }

    /**
     * Subclasses override this to provide specific Trino configuration.
     */
    protected abstract void configureTrino(GenericContainer<?> container);

    private static final class FixedHttpsPortTrinoContainer
            extends GenericContainer<FixedHttpsPortTrinoContainer>
    {
        private FixedHttpsPortTrinoContainer(DockerImageName dockerImageName)
        {
            super(dockerImageName);
        }

        @Override
        protected void configure()
        {
            super.configure();
            addFixedExposedPort(HTTPS_PORT, HTTPS_PORT);
        }
    }

    /**
     * Returns the base config.properties content for OAuth2 authentication.
     */
    protected String getBaseConfigProperties()
    {
        // Configuration based on original singlenode-oauth2 environment
        return """
                node.environment=test
                coordinator=true
                node-scheduler.include-coordinator=true
                http-server.http.port=8080
                query.max-memory=2GB
                query.max-memory-per-node=1.25GB
                discovery.uri=http://localhost:8080
                web-ui.enabled=true
                http-server.authentication.type=oauth2
                http-server.https.port=%d
                http-server.https.enabled=true
                http-server.https.keystore.path=/etc/trino/trino.pem
                http-server.authentication.oauth2.issuer=https://hydra:4444/
                http-server.authentication.oauth2.auth-url=https://hydra:4444/oauth2/auth
                http-server.authentication.oauth2.token-url=https://hydra:4444/oauth2/token
                http-server.authentication.oauth2.jwks-url=https://hydra:4444/.well-known/jwks.json
                http-server.authentication.oauth2.client-id=%s
                http-server.authentication.oauth2.client-secret=%s
                http-server.authentication.oauth2.user-mapping.pattern=(.*)(@.*)?
                http-server.authentication.oauth2.oidc.discovery=false
                oauth2-jwk.http-client.trust-store-path=/etc/trino/hydra.pem
                internal-communication.shared-secret=internal-shared-secret
                catalog.management=dynamic
                query.min-expire-age=1m
                task.info.max-age=1m
                """.formatted(HTTPS_PORT, CLIENT_ID, CLIENT_SECRET);
    }

    private void setupHttpClient()
            throws Exception
    {
        httpClient = createStrictHttpClient();
    }

    private OkHttpClient createStrictHttpClient()
            throws Exception
    {
        int hydraPort = hydra.getMappedPort(4444);
        int trinoPort = trinoContainer.getMappedPort(HTTPS_PORT);
        int consentPort = hydraConsent.getMappedPort(3000);
        InetAddress dockerHostAddress = InetAddress.getByName(trinoContainer.getHost());

        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        KeyStore keyStore = KeyStore.getInstance(truststorePath.toFile(), "123456".toCharArray());
        HandshakeCertificates.Builder certificatesBuilder = new HandshakeCertificates.Builder();
        keyStore.aliases().asIterator().forEachRemaining(alias -> {
            try {
                Certificate certificate = keyStore.getCertificate(alias);
                if (certificate instanceof X509Certificate x509Certificate) {
                    certificatesBuilder.addTrustedCertificate(x509Certificate);
                }
            }
            catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
        });
        HandshakeCertificates certificates = certificatesBuilder.build();
        httpClientBuilder.sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager());
        httpClientBuilder.dns(hostname -> {
            if (hostname.equals(TRINO_HOST) || hostname.equals(HYDRA_HOST) || hostname.equals(HYDRA_CONSENT_HOST)) {
                return List.of(dockerHostAddress);
            }
            return Dns.SYSTEM.lookup(hostname);
        });
        // Handle redirects manually to translate container URLs
        httpClientBuilder.followRedirects(false);
        httpClientBuilder.cookieJar(new JavaNetCookieJar(new CookieManager()));
        httpClientBuilder.addInterceptor(chain -> {
            Request request = chain.request();
            HttpUrl url = request.url();
            Integer mappedPort = switch (url.host()) {
                case HYDRA_HOST -> hydraPort;
                case HYDRA_CONSENT_HOST -> consentPort;
                case TRINO_HOST -> trinoPort;
                default -> null;
            };
            if (mappedPort != null && url.port() != mappedPort) {
                request = request.newBuilder()
                        .url(url.newBuilder().port(mappedPort).build())
                        .build();
            }
            return chain.proceed(request);
        });
        return httpClientBuilder.build();
    }

    // ==================== ProductTestEnvironment Methods ====================

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        Properties properties = createConnectionProperties();
        return DriverManager.getConnection(
                String.format("jdbc:trino://%s:%d",
                        getTrinoHostName(),
                        trinoContainer.getMappedPort(HTTPS_PORT)),
                properties);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        Properties properties = createConnectionProperties();
        properties.setProperty("user", user);
        return DriverManager.getConnection(
                String.format("jdbc:trino://%s:%d",
                        getTrinoHostName(),
                        trinoContainer.getMappedPort(HTTPS_PORT)),
                properties);
    }

    private Properties createConnectionProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("externalAuthentication", "true");
        properties.setProperty("externalAuthenticationTimeout", "30s");
        properties.setProperty("SSLTrustStorePath", truststorePath.toAbsolutePath().toString());
        properties.setProperty("SSLTrustStorePassword", "123456");
        properties.setProperty("dnsResolver", HostMappingDnsResolver.class.getName());
        properties.setProperty("dnsResolverContext", "%s=%s".formatted(
                getTrinoHostName(), trinoContainer.getHost()));
        return properties;
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trinoContainer != null
                ? String.format("jdbc:trino://%s:%d?SSL=true&SSLVerification=NONE&externalAuthentication=true&externalAuthenticationTimeout=30s",
                        trinoContainer.getHost(),
                        trinoContainer.getMappedPort(HTTPS_PORT))
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
        if (trinoContainer != null) {
            trinoContainer.close();
            trinoContainer = null;
        }
        try {
            stopAdditionalServices();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to stop additional OAuth2 services", e);
        }
        if (hydraClientPreparation != null) {
            hydraClientPreparation.close();
            hydraClientPreparation = null;
        }
        if (hydra != null) {
            hydra.close();
            hydra = null;
        }
        if (hydraConsent != null) {
            hydraConsent.close();
            hydraConsent = null;
        }
        if (hydraMigration != null) {
            hydraMigration.close();
            hydraMigration = null;
        }
        if (hydraDb != null) {
            hydraDb.close();
            hydraDb = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
        // Cleanup temp files
        truststorePath = null;
    }

    // ==================== OAuth2 Redirect Handler ====================

    /**
     * Creates a redirect handler that automatically accepts OAuth2 authentication.
     * This simulates a user logging in through the OAuth2 flow.
     * <p>
     * The redirect handler translates URLs from container-internal addresses (hydra:4444)
     * to localhost with the mapped port, since the tests run outside the Docker network.
     * It manually follows redirects to ensure URL translation happens at each step.
     * <p>
     * Each invocation creates a fresh HTTP client with a new cookie jar to avoid
     * state pollution between authentication attempts.
     */
    public Consumer<URI> createRedirectHandler()
    {
        return uri -> {
            OkHttpClient freshClient;
            try {
                freshClient = createStrictHttpClient().newBuilder()
                        .cookieJar(new JavaNetCookieJar(new CookieManager()))
                        .build();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to create OAuth2 client", e);
            }

            String currentUrl = uri.toString();
            try {
                // Follow redirects manually (up to 20 redirects for the OAuth2 flow)
                for (int i = 0; i < 20; i++) {
                    try (Response response = freshClient.newCall(
                            new Request.Builder()
                                    .get()
                                    .url(currentUrl)
                                    .build())
                            .execute()) {
                        int statusCode = response.code();
                        if (statusCode >= 300 && statusCode < 400) {
                            // Follow redirect
                            String location = response.header("Location");
                            checkState(location != null, "Redirect without Location header from %s", currentUrl);
                            currentUrl = location;
                            continue;
                        }
                        String body = response.body().string();
                        checkState(statusCode == 200, "Invalid status %s from %s: %s", statusCode, currentUrl, body);
                        checkState(body.contains("OAuth2 authentication succeeded"), "Invalid response from %s: %s", currentUrl, body);
                        return;
                    }
                }
                throw new IllegalStateException("Too many redirects, last URL: " + currentUrl);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public OkHttpClient getHttpClient()
    {
        return httpClient;
    }

    public String getTrinoHostName()
    {
        return TRINO_HOST;
    }
}
