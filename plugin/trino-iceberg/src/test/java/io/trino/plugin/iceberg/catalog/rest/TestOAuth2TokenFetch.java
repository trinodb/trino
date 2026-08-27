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
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.QuotedETagRestCatalogServlet;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOAuth2TokenFetch
{
    @Test
    void testTokenFetchCountForSessionNone(@TempDir Path warehouseLocation)
            throws Exception
    {
        Catalog backend = backendCatalog(warehouseLocation);
        ExpiringTokenRestCatalogAdapter adapter = new ExpiringTokenRestCatalogAdapter(backend, Duration.ofHours(1));
        TestingHttpServer testServer = createTestServer(adapter);
        testServer.start();

        try (DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.security", "OAUTH2")
                                .put("iceberg.rest-catalog.oauth2.credential", "client_id:client_secret")
                                .put("iceberg.rest-catalog.oauth2.token-refresh-enabled", "true")
                                .put("iceberg.rest-catalog.session", "NONE")
                                .buildOrThrow())
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .disableSchemaInitializer()
                .build()) {
            assertThat(adapter.tokenFetchCount()).isEqualTo(0);

            queryRunner.execute("CREATE SCHEMA test_schema");
            queryRunner.execute("CREATE TABLE test_schema.test_table (id INTEGER, name VARCHAR)");
            queryRunner.execute("INSERT INTO test_schema.test_table VALUES (1, 'alice'), (2, 'bob')");
            queryRunner.execute("SELECT * FROM test_schema.test_table");
            queryRunner.execute("SELECT count(*) FROM test_schema.test_table");
            queryRunner.execute("DROP TABLE test_schema.test_table");
            queryRunner.execute("DROP SCHEMA test_schema");

            assertThat(adapter.tokenFetchCount()).isEqualTo(1);
        }
        finally {
            testServer.stop();
        }
    }

    private static TestingHttpServer createTestServer(ExpiringTokenRestCatalogAdapter adapter)
            throws Exception
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        return new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, new QuotedETagRestCatalogServlet(adapter), ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
    }

    private static final class ExpiringTokenRestCatalogAdapter
            extends RESTCatalogAdapter
    {
        private final AtomicInteger tokenFetchCount = new AtomicInteger();
        private final int tokenExpiresInSeconds;

        public ExpiringTokenRestCatalogAdapter(Catalog delegate, Duration tokenExpiresIn)
        {
            super(delegate);
            this.tokenExpiresInSeconds = toIntExact(tokenExpiresIn.toSeconds());
        }

        @Override
        protected <T extends RESTResponse> T execute(
                HTTPRequest request,
                Class<T> responseType,
                Consumer<ErrorResponse> errorHandler,
                Consumer<Map<String, String>> responseHeaders)
        {
            T response = super.execute(request, responseType, errorHandler, responseHeaders);
            if (response instanceof OAuthTokenResponse tokenResponse) {
                tokenFetchCount.incrementAndGet();
                return responseType.cast(OAuthTokenResponse.builder()
                        .withToken(jwtEncodingExpiry(tokenExpiresInSeconds))
                        .withTokenType(tokenResponse.tokenType())
                        .withIssuedTokenType(tokenResponse.issuedTokenType())
                        .addScopes(tokenResponse.scopes())
                        .setExpirationInSeconds(tokenExpiresInSeconds)
                        .build());
            }
            return response;
        }

        public int tokenFetchCount()
        {
            return tokenFetchCount.get();
        }

        private static String jwtEncodingExpiry(long expiresInSeconds)
        {
            long expiresAtEpochSeconds = System.currentTimeMillis() / 1000 + expiresInSeconds;
            return base64Url("{\"alg\":\"none\"}") + "." + base64Url("{\"exp\":" + expiresAtEpochSeconds + "}") + "." + base64Url("signature");
        }

        private static String base64Url(String value)
        {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(UTF_8));
        }
    }
}
