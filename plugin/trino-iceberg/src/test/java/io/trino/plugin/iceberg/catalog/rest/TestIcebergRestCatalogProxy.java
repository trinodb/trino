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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.Floci;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.createTestingHttpServer;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergRestCatalogProxy
{
    private static final String PROXY_USERNAME = "proxy_user";
    private static final String PROXY_PASSWORD = "proxy_password";
    // Never resolvable, so the catalog can be reached only through the proxy
    private static final String UNREACHABLE_CATALOG_URI = "http://rest-catalog.invalid:8181";

    private final String bucketName = "test-iceberg-rest-proxy-" + randomNameSuffix();
    private final List<JdbcCatalog> backendCatalogs = new CopyOnWriteArrayList<>();
    private Floci floci;

    @BeforeAll
    void setUp()
    {
        floci = new Floci();
        floci.start();
        floci.createBucket(bucketName);
        floci.createRole("test");
    }

    @AfterAll
    void tearDown()
            throws IOException
    {
        for (JdbcCatalog backendCatalog : backendCatalogs) {
            backendCatalog.close();
        }
        backendCatalogs.clear();
        if (floci != null) {
            floci.close();
            floci = null;
        }
    }

    @Test
    void testRestCatalogThroughAuthenticatingProxy()
            throws Exception
    {
        AuthenticatingProxyServlet proxy = new AuthenticatingProxyServlet(new RESTCatalogServlet(new RESTCatalogAdapter(newBackendCatalog())));
        TestingHttpServer proxyServer = createTestingHttpServer(proxy);
        proxyServer.start();

        try (DistributedQueryRunner queryRunner = createQueryRunner(proxyServer, PROXY_PASSWORD, false)) {
            assertTableRoundTrip(queryRunner);
            assertThat(proxy.authenticatedRequestPaths())
                    .anyMatch(path -> path.endsWith("/v1/namespaces"));
        }
        finally {
            proxyServer.stop();
        }
    }

    @Test
    void testProxyConfiguredInCatalogNotOverriddenByCatalogServerConfig()
            throws Exception
    {
        RESTCatalogAdapter adapter = new RESTCatalogAdapter(newBackendCatalog())
        {
            @Override
            protected <T extends RESTResponse> T execute(
                    HTTPRequest request,
                    Class<T> responseType,
                    Consumer<ErrorResponse> errorHandler,
                    Consumer<Map<String, String>> responseHeaders)
            {
                T response = super.execute(request, responseType, errorHandler, responseHeaders);
                if (response instanceof ConfigResponse) {
                    // The catalog server tries to redirect the proxy, and its credentials, to a host of its choosing
                    return responseType.cast(ConfigResponse.builder()
                            .withDefaults(ImmutableMap.of("rest.client.proxy.hostname", "attacker.invalid"))
                            .withOverrides(ImmutableMap.of(
                                    "rest.client.proxy.hostname", "attacker.invalid",
                                    "rest.client.proxy.port", "1",
                                    "rest.client.proxy.username", "attacker",
                                    "rest.client.proxy.password", "attacker"))
                            .build());
                }
                return response;
            }
        };
        AuthenticatingProxyServlet proxy = new AuthenticatingProxyServlet(new RESTCatalogServlet(adapter));
        TestingHttpServer proxyServer = createTestingHttpServer(proxy);
        proxyServer.start();

        try (DistributedQueryRunner queryRunner = createQueryRunner(proxyServer, PROXY_PASSWORD, false)) {
            assertTableRoundTrip(queryRunner);
            assertThat(proxy.authenticatedRequestPaths())
                    .anyMatch(path -> path.endsWith("/v1/namespaces"));
        }
        finally {
            proxyServer.stop();
        }
    }

    @Test
    void testVendedCredentialsThroughAuthenticatingProxy()
            throws Exception
    {
        VendedCredentialsRestCatalogServlet catalogServlet = createVendedCredentialsCatalogServlet();
        AuthenticatingProxyServlet proxy = new AuthenticatingProxyServlet(catalogServlet);
        TestingHttpServer proxyServer = createTestingHttpServer(proxy);
        proxyServer.start();

        try (DistributedQueryRunner queryRunner = createQueryRunner(proxyServer, PROXY_PASSWORD, true)) {
            assertTableRoundTrip(queryRunner);
            assertThat(proxy.authenticatedRequestPaths())
                    .anyMatch(path -> path.endsWith("/v1/namespaces"));
            // The vended credentials expire within the refresh prefetch window, so they are refreshed by a client of their own
            assertThat(catalogServlet.getVendedCredentialsRefreshCount()).isPositive();
            assertThat(proxy.authenticatedRequestPaths())
                    .anyMatch(path -> path.endsWith("/credentials"));
        }
        finally {
            proxyServer.stop();
        }
    }

    @Test
    void testInvalidProxyCredentials()
            throws Exception
    {
        AuthenticatingProxyServlet proxy = new AuthenticatingProxyServlet(new RESTCatalogServlet(new RESTCatalogAdapter(newBackendCatalog())));
        TestingHttpServer proxyServer = createTestingHttpServer(proxy);
        proxyServer.start();

        try {
            try (DistributedQueryRunner queryRunner = createQueryRunner(proxyServer, "wrong_password", false)) {
                assertThatThrownBy(() -> queryRunner.execute("SHOW SCHEMAS"))
                        .hasStackTraceContaining("code: 407");
            }
            assertThat(proxy.authenticatedRequestPaths()).isEmpty();
        }
        finally {
            proxyServer.stop();
        }
    }

    private static void assertTableRoundTrip(DistributedQueryRunner queryRunner)
    {
        queryRunner.execute("CREATE SCHEMA test_schema");
        queryRunner.execute("CREATE TABLE test_schema.test_table (id INTEGER)");
        queryRunner.execute("INSERT INTO test_schema.test_table VALUES (1), (2)");
        assertThat(queryRunner.execute("SELECT count(*) FROM test_schema.test_table").getOnlyValue()).isEqualTo(2L);
        queryRunner.execute("DROP TABLE test_schema.test_table");
        queryRunner.execute("DROP SCHEMA test_schema");
    }

    private JdbcCatalog newBackendCatalog()
            throws IOException
    {
        JdbcCatalog backend = buildBackendCatalog();
        backendCatalogs.add(backend);
        return backend;
    }

    private VendedCredentialsRestCatalogServlet createVendedCredentialsCatalogServlet()
            throws IOException
    {
        VendedCredentialsRestCatalogAdapter adapter = new VendedCredentialsRestCatalogAdapter(newBackendCatalog())
        {
            @Override
            protected Map<String, String> getVendedCredentialsConfig(String restServerUri)
            {
                return ImmutableMap.<String, String>builder()
                        .putAll(s3CredentialsConfig(Instant.now().plusSeconds(60)))
                        .put(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, UNREACHABLE_CATALOG_URI + "/credentials")
                        // Pins that the configured proxy wins over proxy entries from the catalog server
                        .put("rest.client.proxy.hostname", "attacker.invalid")
                        .buildOrThrow();
            }
        };
        return new VendedCredentialsRestCatalogServlet(adapter)
        {
            @Override
            protected String getPrefix()
            {
                return "s3://";
            }

            @Override
            protected Map<String, String> getCredentialsConfig()
            {
                return s3CredentialsConfig(Instant.now().plus(1, ChronoUnit.HOURS));
            }
        };
    }

    private Map<String, String> s3CredentialsConfig(Instant expiresAt)
    {
        Credentials sessionCredentials = getSessionCredentials();
        return ImmutableMap.<String, String>builder()
                .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                .put(AwsClientProperties.REFRESH_CREDENTIALS_ENABLED, "true")
                .put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(expiresAt.toEpochMilli()))
                .buildOrThrow();
    }

    private Credentials getSessionCredentials()
    {
        try (StsClient stsClient = StsClient.builder().applyMutation(floci::updateClient).build()) {
            return stsClient.assumeRole(AssumeRoleRequest.builder()
                    .roleArn("arn:aws:iam::000000000000:role/test")
                    .roleSessionName("test")
                    .build()).credentials();
        }
    }

    private JdbcCatalog buildBackendCatalog()
            throws IOException
    {
        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(CatalogProperties.WAREHOUSE_LOCATION, "s3://%s/default/".formatted(bucketName))
                .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
                .put(S3FileIOProperties.ACCESS_KEY_ID, FLOCI_ACCESS_KEY)
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, FLOCI_SECRET_KEY)
                .put(S3FileIOProperties.ENDPOINT, floci.endpoint().toString())
                .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                .buildOrThrow());
        return catalog;
    }

    private DistributedQueryRunner createQueryRunner(TestingHttpServer proxyServer, String proxyPassword, boolean vendedCredentialsEnabled)
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "rest")
                .put("iceberg.rest-catalog.uri", UNREACHABLE_CATALOG_URI)
                .put("iceberg.rest-catalog.vended-credentials-enabled", String.valueOf(vendedCredentialsEnabled))
                .put("iceberg.rest-catalog.http-proxy", HostAndPort.fromParts(proxyServer.getBaseUrl().getHost(), proxyServer.getBaseUrl().getPort()).toString())
                .put("iceberg.rest-catalog.http-proxy.username", PROXY_USERNAME)
                .put("iceberg.rest-catalog.http-proxy.password", proxyPassword)
                .put("fs.s3.enabled", "true")
                .put("s3.endpoint", floci.endpoint().toString())
                .put("s3.region", FLOCI_REGION)
                .put("s3.path-style-access", "true");
        if (!vendedCredentialsEnabled) {
            properties.put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                    .put("s3.aws-secret-key", FLOCI_SECRET_KEY);
        }
        return IcebergQueryRunner.builder()
                .setIcebergProperties(properties.buildOrThrow())
                .disableSchemaInitializer()
                .build();
    }

    /**
     * Plain HTTP forward proxy requiring Basic authentication. Instead of forwarding requests
     * to the host named in the request line, it serves them from the in-process REST catalog.
     */
    private static final class AuthenticatingProxyServlet
            extends HttpServlet
    {
        private static final String EXPECTED_AUTHORIZATION = "Basic " + Base64.getEncoder().encodeToString((PROXY_USERNAME + ":" + PROXY_PASSWORD).getBytes(UTF_8));

        private final RESTCatalogServlet catalogServlet;
        private final List<String> authenticatedRequestPaths = new CopyOnWriteArrayList<>();

        private AuthenticatingProxyServlet(RESTCatalogServlet catalogServlet)
        {
            this.catalogServlet = catalogServlet;
        }

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException
        {
            if (!EXPECTED_AUTHORIZATION.equals(request.getHeader("Proxy-Authorization"))) {
                response.setHeader("Proxy-Authenticate", "Basic realm=\"proxy\"");
                response.sendError(HttpServletResponse.SC_PROXY_AUTHENTICATION_REQUIRED);
                return;
            }
            authenticatedRequestPaths.add(request.getRequestURI());
            catalogServlet.service(request, response);
        }

        private List<String> authenticatedRequestPaths()
        {
            return authenticatedRequestPaths;
        }
    }
}
