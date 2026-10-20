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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RemoteSigningConfigParser;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.rest.responses.RemoteSignResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningAuthentication.AUTHENTICATION_PROPERTY;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningContext.EXTRA_CREDENTIALS_PROPERTY;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpMethod.GET;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner.REGION_NAME;

final class TestIcebergRestCatalogS3RemoteSigningAuthentication
{
    private static final String EXPIRED_TOKEN = "eyJhbGciOiJub25lIn0.eyJleHAiOjF9.c2lnbmF0dXJl";
    private static final String CATALOG_TOKEN = "eyJhbGciOiJub25lIn0.eyJleHAiOjQxMDI0NDQ4MDAsInN1YiI6ImNhdGFsb2cifQ.c2lnbmF0dXJl";
    private static final String RENEWED_CATALOG_TOKEN = "eyJhbGciOiJub25lIn0.eyJleHAiOjQxMDI0NDQ4MDAsInN1YiI6InJlbmV3ZWQtY2F0YWxvZyJ9.c2lnbmF0dXJl";
    private static final SignRequest<AwsCredentialsIdentity> REQUEST = SignRequest.<AwsCredentialsIdentity>builder(AnonymousCredentialsProvider.create().resolveCredentials())
            .request(SdkHttpRequest.builder().method(GET).uri(URI.create("https://storage.example/object")).build())
            .putProperty(REGION_NAME, "us-east-1")
            .build();

    @Test
    void testAuthenticationExcludesUnrelatedCatalogProperties()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("table", ImmutableMap.of());
            server.servlet.signingAuthorization = "Bearer catalog-token";
            server.initialize(ImmutableMap.of(
                    OAuth2Properties.AUDIENCE, "signer",
                    OAuth2Properties.RESOURCE, "storage",
                    "header.x-signer-context", "catalog",
                    "unrelated-option", "value",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "unused-storage-secret"));
            Table table = server.loadTable("table");
            String authenticationJson = table.io().properties().get(AUTHENTICATION_PROPERTY);
            assertThat(authenticationJson).doesNotContain("unrelated-option", "unused-storage-secret", S3FileIOProperties.REMOTE_SIGNING_ENABLED);
            IcebergRestCatalogS3RemoteSigningAuthentication authentication = IcebergRestCatalogS3RemoteSigningAuthentication.fromJson(authenticationJson);
            assertThat(authentication.credential()).contains("catalog-client:catalog-secret");
            assertThat(authentication.oauth2ServerUri()).isEqualTo(server.uri("/catalog/v1/oauth/tokens").toString());
            assertThat(authentication.audience()).contains("signer");
            assertThat(authentication.resource()).contains("storage");
            assertThat(authentication.headers()).containsEntry("x-signer-context", "catalog");
            server.clearRequests();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.body()).contains("audience=signer", "resource=storage");
                assertThat(request.signerContext()).isEqualTo("catalog");
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCatalogConfigControlsOAuthAuthentication(boolean overrides)
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            Map<String, String> catalogAuthentication = ImmutableMap.of(
                    OAuth2Properties.OAUTH2_SERVER_URI, server.uri("/tenant-token").toString(),
                    OAuth2Properties.SCOPE, "tenant-scope",
                    "header.x-signer-context", "tenant-catalog");
            ConfigResponse.Builder config = ConfigResponse.builder();
            ImmutableMap.Builder<String, String> localProperties = ImmutableMap.<String, String>builder()
                    .put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
            if (overrides) {
                config.withOverrides(catalogAuthentication);
                localProperties
                        .put(OAuth2Properties.OAUTH2_SERVER_URI, server.uri("/bootstrap-token").toString())
                        .put(OAuth2Properties.SCOPE, "metadata-scope")
                        .put("header.x-signer-context", "bootstrap-catalog");
            }
            else {
                config.withDefaults(catalogAuthentication);
            }
            server.servlet.config = config.build();
            server.servlet.tableProperties.put("table", ImmutableMap.of(
                    OAuth2Properties.OAUTH2_SERVER_URI, server.uri("/table-token").toString(),
                    OAuth2Properties.SCOPE, "table-scope"));
            server.servlet.signingAuthorization = "Bearer tenant-token";
            server.initialize(localProperties.buildOrThrow());
            Table table = server.loadTable("table");
            server.clearRequests();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.path()).isEqualTo("/tenant-token");
                assertThat(request.body()).contains("grant_type=client_credentials", "client_id=catalog-client", "scope=tenant-scope");
                assertThat(request.body()).doesNotContain("scope=metadata-scope", "scope=table-scope");
                assertThat(request.authorization()).isNull();
                assertThat(request.signerContext()).isEqualTo("tenant-catalog");
            });
            assertThat(server.servlet.signingCalls).singleElement().satisfies(call -> {
                assertThat(call.path()).isEqualTo("/catalog/v1/namespaces/test/tables/table/sign");
                assertThat(call.authorization()).isEqualTo("Bearer tenant-token");
                assertThat(call.signerContext()).isEqualTo("table");
            });
        }
    }

    @Test
    void testExplicitTableCredentialRefreshesExpiredToken()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("table", ImmutableMap.of(
                    OAuth2Properties.TOKEN, EXPIRED_TOKEN,
                    OAuth2Properties.CREDENTIAL, "table-client:table-secret"));
            server.initialize(ImmutableMap.of(
                    OAuth2Properties.TOKEN, "catalog-token",
                    OAuth2Properties.TOKEN_EXPIRES_IN_MS, "3600000"));
            Table table = server.loadTable("table");
            server.clearRequests();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.path()).isEqualTo("/catalog/v1/oauth/tokens");
                assertThat(request.body()).contains("grant_type=client_credentials", "client_id=table-client", "client_secret=table-secret");
                assertThat(request.body()).doesNotContain("client_id=catalog-client");
                assertThat(request.authorization()).isNull();
            });
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer table-token");
        }
    }

    @Test
    void testTableCredentialOverridesInheritedCatalogToken()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("table", ImmutableMap.of(OAuth2Properties.CREDENTIAL, "table-client:table-secret"));
            server.initialize(ImmutableMap.of(OAuth2Properties.TOKEN, "catalog-token"));
            Table table = server.loadTable("table");
            assertThat(table.io().properties()).containsEntry(OAuth2Properties.TOKEN, "catalog-token");
            server.clearRequests();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.path()).isEqualTo("/catalog/v1/oauth/tokens");
                assertThat(request.body()).contains("grant_type=client_credentials", "client_id=table-client", "client_secret=table-secret");
                assertThat(request.authorization()).isNull();
            });
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer table-token");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExpiredTableTokenCannotUseInheritedCatalogCredential(boolean overrideCatalogCredential)
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            if (overrideCatalogCredential) {
                server.servlet.config = ConfigResponse.builder()
                        .withOverride(OAuth2Properties.CREDENTIAL, "tenant-client:tenant-secret")
                        .build();
            }
            server.servlet.tableProperties.put("table", ImmutableMap.of(OAuth2Properties.TOKEN, EXPIRED_TOKEN));
            server.initialize(ImmutableMap.of());
            Table table = server.loadTable("table");
            assertThat(table.io().properties()).containsEntry(OAuth2Properties.CREDENTIAL, overrideCatalogCredential ? "tenant-client:tenant-secret" : "catalog-client:catalog-secret");
            server.clearRequests();

            assertTrinoExceptionThrownBy(() -> server.sign(table))
                    .hasErrorCode(ICEBERG_CATALOG_ERROR)
                    .hasMessage("Failed to remotely sign S3 request")
                    .cause()
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.tokenRequests).isEmpty();
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer " + EXPIRED_TOKEN);
        }
    }

    @Test
    void testExplicitTableCredentialCanEqualEffectiveCatalogCredential()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.config = ConfigResponse.builder()
                    .withOverride(OAuth2Properties.CREDENTIAL, "tenant-client:tenant-secret")
                    .build();
            server.servlet.tableProperties.put("table", ImmutableMap.of(
                    OAuth2Properties.TOKEN, EXPIRED_TOKEN,
                    OAuth2Properties.CREDENTIAL, "tenant-client:tenant-secret"));
            server.servlet.signingAuthorization = "Bearer tenant-token";
            server.initialize(ImmutableMap.of());
            Table table = server.loadTable("table");
            server.clearRequests();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.path()).isEqualTo("/catalog/v1/oauth/tokens");
                assertThat(request.body()).contains("grant_type=client_credentials", "client_id=tenant-client", "client_secret=tenant-secret");
                assertThat(request.authorization()).isNull();
            });
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer tenant-token");
        }
    }

    @Test
    void testRefreshedTokenIsNotSharedWithTableWithoutCredential()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("delegated", ImmutableMap.of(
                    OAuth2Properties.TOKEN, EXPIRED_TOKEN,
                    OAuth2Properties.CREDENTIAL, "table-client:table-secret"));
            server.servlet.tableProperties.put("token_only", ImmutableMap.of(OAuth2Properties.TOKEN, EXPIRED_TOKEN));
            server.initialize(ImmutableMap.of());
            Table delegated = server.loadTable("delegated");
            server.clearRequests();

            assertThat(server.sign(delegated).request().firstMatchingHeader("Authorization")).contains("signed");
            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request ->
                    assertThat(request.body()).contains("client_id=table-client"));
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer table-token");

            Table tokenOnly = server.loadTable("token_only");
            server.clearRequests();

            assertTrinoExceptionThrownBy(() -> server.sign(tokenOnly))
                    .hasErrorCode(ICEBERG_CATALOG_ERROR)
                    .hasMessage("Failed to remotely sign S3 request")
                    .cause()
                    .isInstanceOf(ForbiddenException.class)
                    .hasMessage("Forbidden: Access denied");

            assertThat(server.servlet.tokenRequests).isEmpty();
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer " + EXPIRED_TOKEN);
        }
    }

    @Test
    void testCatalogCredentialTakesPrecedenceOverConfiguredToken()
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("table", ImmutableMap.of());
            server.servlet.catalogAuthorization = "Bearer catalog-token";
            server.servlet.signingAuthorization = "Bearer catalog-token";
            server.initialize(ImmutableMap.of(OAuth2Properties.TOKEN, "expired-opaque-token"));
            Table table = server.loadTable("table");
            // The catalog session uses the token fetched with the credential, not the configured token
            assertThat(server.servlet.metadataAuthorizations).containsExactly("Bearer catalog-token");
            server.clearRequests();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.authorization()).isNull();
                assertThat(request.body()).contains("grant_type=client_credentials", "client_id=catalog-client");
            });
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer catalog-token");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOpaqueCatalogTokenDoesNotScheduleRefresh(boolean initialExpiryConfigured)
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("table", ImmutableMap.of());
            server.servlet.catalogAuthorization = "Bearer opaque-catalog-token";
            server.servlet.signingAuthorization = "Bearer opaque-catalog-token";
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put(OAuth2Properties.TOKEN, "opaque-catalog-token")
                    .put(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "true");
            if (initialExpiryConfigured) {
                properties.put(OAuth2Properties.TOKEN_EXPIRES_IN_MS, "3600000");
            }
            server.initializeCatalog(properties.buildOrThrow());
            Table table = server.loadTable("table");
            AuthConfig authentication = IcebergRestCatalogS3RemoteSigningAuthentication.fromJson(table.io().properties().get(AUTHENTICATION_PROPERTY)).toAuthConfig();
            assertThat(authentication.keepRefreshed()).isTrue();
            assertThat(authentication.exchangeEnabled()).isTrue();
            // Iceberg schedules token refresh only when an expiry is known.
            assertThat(authentication.expiresAtMillis()).isNull();

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.tokenRequests).isEmpty();
            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer opaque-catalog-token");
        }
    }

    @Test
    void testConfiguredTokenLifetimeAppliesWithoutExpiryClaim()
    {
        Map<String, String> catalogProperties = ImmutableMap.of(CatalogProperties.URI, "http://localhost/catalog");
        long start = System.currentTimeMillis();
        AuthConfig opaqueToken = IcebergRestCatalogS3RemoteSigningAuthentication.create(
                        catalogProperties,
                        ImmutableMap.of(OAuth2Properties.TOKEN, "opaque-table-token", OAuth2Properties.TOKEN_EXPIRES_IN_MS, "60000"),
                        AuthSession.EMPTY)
                .toAuthConfig();
        assertThat(opaqueToken.expiresAtMillis()).isBetween(start + 60_000, System.currentTimeMillis() + 60_000);

        AuthConfig tokenWithExpiryClaim = IcebergRestCatalogS3RemoteSigningAuthentication.create(
                        catalogProperties,
                        ImmutableMap.of(OAuth2Properties.TOKEN, EXPIRED_TOKEN, OAuth2Properties.TOKEN_EXPIRES_IN_MS, "60000"),
                        AuthSession.EMPTY)
                .toAuthConfig();
        assertThat(tokenWithExpiryClaim.expiresAtMillis()).isEqualTo(1000);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSigningUsesRenewedCatalogToken(boolean signedBeforeRefresh)
            throws Exception
    {
        try (TestServer server = new TestServer()) {
            server.servlet.tableProperties.put("before_refresh", ImmutableMap.of());
            server.servlet.tableProperties.put("after_refresh", ImmutableMap.of());
            server.servlet.catalogAuthorization = "Bearer " + CATALOG_TOKEN;
            server.servlet.signingAuthorization = "Bearer " + CATALOG_TOKEN;
            server.initializeTokenOnly(CATALOG_TOKEN);
            if (signedBeforeRefresh) {
                assertThat(server.sign(server.loadTable("before_refresh")).request().firstMatchingHeader("Authorization")).contains("signed");
            }
            server.clearRequests();

            server.refreshCatalogToken();

            assertThat(server.servlet.tokenRequests).singleElement().satisfies(request -> {
                assertThat(request.path()).isEqualTo("/catalog/v1/oauth/tokens");
                assertThat(request.authorization()).isEqualTo("Bearer " + CATALOG_TOKEN);
                assertThat(request.body()).contains(
                        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange",
                        "subject_token=" + CATALOG_TOKEN);
                assertThat(request.body()).doesNotContain("client_id=", "client_secret=");
            });
            Table table = server.loadTable("after_refresh");
            assertThat(server.servlet.metadataAuthorizations).containsExactly("Bearer " + RENEWED_CATALOG_TOKEN);

            assertThat(server.sign(table).request().firstMatchingHeader("Authorization")).contains("signed");

            assertThat(server.servlet.signingCalls).extracting(SigningCall::authorization).containsExactly("Bearer " + RENEWED_CATALOG_TOKEN);
        }
    }

    private static final class TestServer
            implements AutoCloseable
    {
        private final AutoCloseableCloser closer = AutoCloseableCloser.create();
        private final CatalogServlet servlet = new CatalogServlet();
        private final TestingHttpServer server;
        private RESTSessionCatalog catalog;
        private TestingOAuth2Manager authManager;
        private IcebergRestCatalogS3RemoteSignerProvider signerProvider;

        public TestServer()
                throws Exception
        {
            NodeInfo nodeInfo = new NodeInfo(new NodeConfig()
                    .setEnvironment("test")
                    .setNodeInternalAddress("127.0.0.1"));
            HttpServerConfig config = new HttpServerConfig().setHttpEnabled(true);
            HttpServerInfo serverInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
            server = new TestingHttpServer("remote-signing-authentication", serverInfo, nodeInfo, config, servlet, ServerFeature.builder().build());
            closer.register(server::stop);
            server.start();
        }

        public URI uri(String path)
        {
            return server.getBaseUrl().resolve(path);
        }

        public void initialize(Map<String, String> properties)
        {
            initializeCatalog(ImmutableMap.<String, String>builder()
                    .put(OAuth2Properties.CREDENTIAL, "catalog-client:catalog-secret")
                    .putAll(properties)
                    .buildKeepingLast());
        }

        public void initializeTokenOnly(String token)
        {
            initializeCatalog(ImmutableMap.of(
                    OAuth2Properties.TOKEN, token,
                    OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "true"));
        }

        private void initializeCatalog(Map<String, String> properties)
        {
            Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                    .put(CatalogProperties.URI, uri("/catalog").toString())
                    .put(AuthProperties.AUTH_TYPE, TestingOAuth2Manager.class.getName())
                    .put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "true")
                    .put(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "false")
                    .put("rest.client.max-retries", "1")
                    .put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "true")
                    .putAll(properties)
                    .buildKeepingLast();
            catalog = closer.register(new RESTSessionCatalog(
                    IcebergRestCatalogS3RemoteSigningClient::new,
                    (_, config) -> new TestingFileIO(config)));
            String catalogName = uri("/catalog").toString();
            try {
                catalog.initialize(catalogName, catalogProperties);
            }
            finally {
                authManager = TestingOAuth2Manager.MANAGERS.remove(catalogName);
            }
            signerProvider = closer.register(new IcebergRestCatalogS3RemoteSignerProvider(catalogProperties));
        }

        public void refreshCatalogToken()
        {
            assertThat(authManager.catalogSession.refresh(authManager.refreshClient)).isNotNull();
        }

        public Table loadTable(String name)
        {
            return catalog.loadTable(SessionContext.createEmpty(), TableIdentifier.of("test", name));
        }

        public SignedRequest sign(Table table)
        {
            ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                    .withExtraCredentials(ImmutableMap.of(EXTRA_CREDENTIALS_PROPERTY, IcebergRestCatalogS3RemoteSigningContext.fromFileIoProperties(table.io().properties()).toJson()))
                    .build();
            return signerProvider.getSigner(identity).orElseThrow().sign(REQUEST);
        }

        public void clearRequests()
        {
            servlet.tokenRequests.clear();
            servlet.signingCalls.clear();
            servlet.metadataAuthorizations.clear();
        }

        @Override
        public void close()
                throws Exception
        {
            closer.close();
        }
    }

    private static final class CatalogServlet
            extends HttpServlet
    {
        private final Map<String, Map<String, String>> tableProperties = new ConcurrentHashMap<>();
        private final List<TokenRequest> tokenRequests = new CopyOnWriteArrayList<>();
        private final List<SigningCall> signingCalls = new CopyOnWriteArrayList<>();
        private final List<String> metadataAuthorizations = new CopyOnWriteArrayList<>();
        private volatile ConfigResponse config = ConfigResponse.builder().build();
        private volatile String catalogAuthorization;
        private volatile String signingAuthorization = "Bearer table-token";

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setContentType("application/json");
            if (request.getRequestURI().equals("/catalog/v1/config")) {
                response.getWriter().write(ConfigResponseParser.toJson(config));
                return;
            }
            String tableName = request.getRequestURI().substring(request.getRequestURI().lastIndexOf('/') + 1);
            Map<String, String> properties = tableProperties.get(tableName);
            if (properties == null) {
                response.sendError(404);
                return;
            }
            String authorization = request.getHeader("Authorization");
            metadataAuthorizations.add(authorization);
            if (catalogAuthorization != null && !catalogAuthorization.equals(authorization)) {
                forbidden(response);
                return;
            }
            response.getWriter().write(LoadTableResponseParser.toJson(LoadTableResponse.builder()
                    .withTableMetadata(TableMetadata.newTableMetadata(
                            new Schema(Types.NestedField.optional(1, "value", Types.IntegerType.get())),
                            PartitionSpec.unpartitioned(),
                            "s3://bucket/" + tableName,
                            ImmutableMap.of()))
                    .addAllConfig(properties)
                    .withRemoteSigningConfig(RemoteSigningConfigParser.fromJson("{\"headers\":{\"X-Signer-Context\":[\"table\"]}}"))
                    .build()));
        }

        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setContentType("application/json");
            String path = request.getRequestURI();
            String body = new String(request.getInputStream().readAllBytes(), UTF_8);
            String authorization = request.getHeader("Authorization");
            String signerContext = request.getHeader("X-Signer-Context");
            if (ImmutableList.of("/bootstrap-token", "/tenant-token", "/catalog/v1/oauth/tokens").contains(path)) {
                tokenRequests.add(new TokenRequest(path, authorization, signerContext, body));
                String token = "catalog-token";
                if (body.contains("subject_token=")) {
                    if (!body.contains("subject_token=" + CATALOG_TOKEN) || !("Bearer " + CATALOG_TOKEN).equals(authorization)) {
                        forbidden(response);
                        return;
                    }
                    token = RENEWED_CATALOG_TOKEN;
                    catalogAuthorization = "Bearer " + RENEWED_CATALOG_TOKEN;
                    signingAuthorization = "Bearer " + RENEWED_CATALOG_TOKEN;
                }
                else if (path.equals("/tenant-token")) {
                    if (!body.contains("scope=tenant-scope") || !"tenant-catalog".equals(signerContext)) {
                        forbidden(response);
                        return;
                    }
                    token = "tenant-token";
                }
                else if (body.contains("client_id=table-client")) {
                    token = "table-token";
                }
                else if (body.contains("client_id=tenant-client")) {
                    token = "tenant-token";
                }
                response.getWriter().write("{\"access_token\":\"" + token + "\",\"token_type\":\"bearer\",\"expires_in\":3600}");
                return;
            }
            if (!path.endsWith("/sign")) {
                response.sendError(404);
                return;
            }
            RemoteSignRequest signingRequest = RemoteSignRequestParser.fromJson(body);
            signingCalls.add(new SigningCall(path, authorization, signerContext));
            if (!signingAuthorization.equals(authorization)) {
                forbidden(response);
                return;
            }
            response.getWriter().write(RemoteSignResponseParser.toJson(ImmutableRemoteSignResponse.builder()
                    .uri(signingRequest.uri())
                    .putHeaders("Authorization", ImmutableList.of("signed"))
                    .build()));
        }

        private static void forbidden(HttpServletResponse response)
                throws IOException
        {
            response.setStatus(403);
            response.getWriter().write("{\"error\":{\"message\":\"Access denied\",\"type\":\"ForbiddenException\",\"code\":403}}");
        }
    }

    public static final class TestingOAuth2Manager
            extends SharedSessionOAuth2Manager
    {
        private static final Map<String, TestingOAuth2Manager> MANAGERS = new ConcurrentHashMap<>();

        private OAuth2Util.AuthSession catalogSession;
        private RESTClient refreshClient;

        public TestingOAuth2Manager(String name)
        {
            super(name);
            MANAGERS.put(name, this);
        }

        @Override
        public OAuth2Util.AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties)
        {
            catalogSession = super.catalogSession(sharedClient, properties);
            refreshClient = sharedClient.withAuthSession(AuthSession.EMPTY);
            return catalogSession;
        }
    }

    private record TestingFileIO(Map<String, String> properties)
            implements FileIO
    {
        @Override
        public InputFile newInputFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path)
        {
            throw new UnsupportedOperationException();
        }
    }

    private record TokenRequest(String path, String authorization, String signerContext, String body) {}

    private record SigningCall(String path, String authorization, String signerContext) {}
}
